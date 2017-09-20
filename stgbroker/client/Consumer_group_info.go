package client

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	set "github.com/deckarep/golang-set"
	"strings"
)

// ConsumerGroupInfo 整个Consumer Group信息
// Author gaoyanlei
// Since 2017/8/17
type ConsumerGroupInfo struct {
	GroupName           string
	SubscriptionTable   *sync.Map // key:Topic, val:SubscriptionData
	ConnTable           *sync.Map // key: Channel val: ClientChannelInfo
	ConsumeType         heartbeat.ConsumeType
	MessageModel        heartbeat.MessageModel
	ConsumeFromWhere    heartbeat.ConsumeFromWhere
	lastUpdateTimestamp int64
}

func NewConsumerGroupInfo(groupName string, consumeType heartbeat.ConsumeType, messageModel heartbeat.MessageModel,
	consumeFromWhere heartbeat.ConsumeFromWhere) *ConsumerGroupInfo {
	var ConsumerGroupInfo = new(ConsumerGroupInfo)
	ConsumerGroupInfo.SubscriptionTable = sync.NewMap()
	ConsumerGroupInfo.ConnTable = sync.NewMap()
	ConsumerGroupInfo.GroupName = groupName
	ConsumerGroupInfo.ConsumeType = consumeType
	ConsumerGroupInfo.MessageModel = messageModel
	ConsumerGroupInfo.ConsumeFromWhere = consumeFromWhere
	return ConsumerGroupInfo
}

func (cg *ConsumerGroupInfo) FindSubscriptionData(topic string) *heartbeat.SubscriptionData {
	value, err := cg.SubscriptionTable.Get(topic)
	if err != nil {
		return nil
	}

	if subscriptionData, ok := value.(*heartbeat.SubscriptionData); ok {
		return subscriptionData
	}

	return nil
}

func (cg *ConsumerGroupInfo) UpdateChannel(infoNew netm.Context, consumeType heartbeat.ConsumeType,
	messageModel heartbeat.MessageModel, consumeFromWhere heartbeat.ConsumeFromWhere) bool {
	// TODO; key - value不統一
	updated := false
	cg.ConsumeType = consumeType
	cg.MessageModel = messageModel
	cg.ConsumeFromWhere = consumeFromWhere
	infoOld, err := cg.ConnTable.Get(infoNew.LocalAddr().String())
	if infoOld == nil || err != nil {
		prev, err := cg.ConnTable.Put(infoNew.LocalAddr().String(), infoNew)
		if prev == nil || err != nil {
			logger.Infof("new consumer connected, group: %s %v %v channel: %s", cg.GroupName, consumeType,
				messageModel, infoNew.LocalAddr().String())
			updated = true
		}
		infoOld = infoNew
	} else {
		if infoold, ok := infoOld.(netm.Context); ok {
			if !strings.EqualFold(infoNew.LocalAddr().String(), infoold.LocalAddr().String()) {
				logger.Errorf(
					"[BUG] consumer channel exist in broker, but clientId not equal. GROUP: %s OLD: %s NEW: %s ",
					cg.GroupName,                 //
					infoold.LocalAddr().String(), //
					infoNew.LocalAddr().String())
				cg.ConnTable.Put(infoNew.LocalAddr().String(), infoNew)
			}

		}
	}
	//cg.lastUpdateTimestamp = System.currentTimeMillis();
	//infoOld.setLastUpdateTimestamp(this.lastUpdateTimestamp);
	return updated
}

// doChannelCloseEvent 关闭通道
// Author rongzhihong
// Since 2017/9/11
func (cg *ConsumerGroupInfo) doChannelCloseEvent(remoteAddr string, ctx netm.Context) bool {
	info, err := cg.ConnTable.Remove(ctx)
	if err != nil {
		logger.Error(err)
		return false
	}
	if info != nil {
		logger.Warnf("NETTY EVENT: remove not active channel[%v] from ConsumerGroupInfo groupChannelTable, consumer group: %s",
			info, cg.GroupName)
		return true
	}
	return false
}

// getAllChannel 获得所有通道
// Author rongzhihong
// Since 2017/9/11
func (cg *ConsumerGroupInfo) GetAllChannel() []netm.Context {
	result := []netm.Context{}
	iterator := cg.ConnTable.Iterator()
	for iterator.HasNext() {
		key, _, _ := iterator.Next()
		if channel, ok := key.(netm.Context); ok {
			result = append(result, channel)
		}
	}
	return result
}

// getAllChannel 获得所有客户端ID
// Author rongzhihong
// Since 2017/9/14
func (cg *ConsumerGroupInfo) GetAllClientId() []string {
	result := []string{}

	iterator := cg.ConnTable.Iterator()
	for iterator.HasNext() {
		_, value, _ := iterator.Next()
		if channel, ok := value.(*ChannelInfo); ok {
			result = append(result, channel.ClientId)
		}
	}

	return result
}

// UnregisterChannel 注销通道
// Author rongzhihong
// Since 2017/9/14
func (cg *ConsumerGroupInfo) UnregisterChannel(clientChannelInfo *ChannelInfo) {
	old, _ := cg.ConnTable.Remove(clientChannelInfo.Context)
	if old != nil {
		logger.Infof("unregister a consumer[%s] from consumerGroupInfo %v", cg.GroupName, old)
	}
}

// UpdateSubscription 更新订阅
// Author rongzhihong
// Since 2017/9/17
func (cg *ConsumerGroupInfo) UpdateSubscription(subList set.Set) bool {
	updated := false

	// 增加新的订阅关系
	iterator := subList.Iterator()
	for item := range iterator.C {
		if sub, ok := item.(*heartbeat.SubscriptionData); ok {
			old, _ := cg.SubscriptionTable.Get(sub.Topic)
			if old == nil {
				prev, _ := cg.SubscriptionTable.Put(sub.Topic, sub)
				if prev == nil {
					updated = true
					logger.Infof("subscription changed, add new topic, group: %s %#v", cg.GroupName, sub)
				}
				continue
			}

			if oldSub, ok := old.(*heartbeat.SubscriptionData); ok {
				if sub.SubVersion > oldSub.SubVersion {
					if cg.ConsumeType == heartbeat.CONSUME_PASSIVELY {
						logger.Infof("subscription changed, group: %s OLD: %#v NEW: %#v", cg.GroupName, old, sub)
					}
					cg.SubscriptionTable.Put(sub.Topic, sub)
				}
			}
		}
	}

	// 删除老的订阅关系
	subIt := cg.SubscriptionTable.Iterator()
	for subIt.HasNext() {
		exist := false
		oldTopic, oldValue, _ := subIt.Next()

		for subItem := range subList.Iterator().C {
			if sub, ok := subItem.(*heartbeat.SubscriptionData); ok {
				if oldTopic, ok := oldTopic.(string); ok && strings.EqualFold(sub.Topic, oldTopic) {
					exist = true
					break
				}
			}
		}

		if !exist {
			logger.Warnf("subscription changed, group: %s remove topic %s %v", cg.GroupName, oldTopic, oldValue)
			subIt.Remove()
			updated = true
		}
	}

	cg.lastUpdateTimestamp = timeutil.CurrentTimeMillis()
	return updated
}

// FindChannel 根据clientId获得通道
// Author rongzhihong
// Since 2017/9/17
func (cg *ConsumerGroupInfo) FindChannel(clientId string) *ChannelInfo {
	iterator := cg.ConnTable.Iterator()
	for iterator.HasNext() {
		_, value, _ := iterator.Next()
		if info, ok := value.(*ChannelInfo); ok {
			if strings.EqualFold(info.ClientId, clientId) {
				return info
			}
		}
	}
	return nil
}

// SubscriptionTableToMap SubscriptionTable To Map
// Author rongzhihong
// Since 2017/9/17
func (cg *ConsumerGroupInfo) SubscriptionTableToMap() map[interface{}]interface{} {
	subscriptionDataMap := map[interface{}]interface{}{}
	iterator := cg.SubscriptionTable.Iterator()
	for iterator.HasNext() {
		k, v, _ := iterator.Next()
		subscriptionDataMap[k] = v
	}
	return subscriptionDataMap
}

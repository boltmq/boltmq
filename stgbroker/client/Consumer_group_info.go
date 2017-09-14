package client

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"net"
	"strings"
)

// ConsumerGroupInfo 整个Consumer Group信息
// Author gaoyanlei
// Since 2017/8/17
type ConsumerGroupInfo struct {
	GroupName           string
	SubscriptionTable   *sync.Map
	ConnTable           *sync.Map
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

func (cg *ConsumerGroupInfo) UpdateChannel(infoNew net.Conn, consumeType heartbeat.ConsumeType,
	messageModel heartbeat.MessageModel, consumeFromWhere heartbeat.ConsumeFromWhere) bool {
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
		if infoold, ok := infoOld.(net.Conn); ok {
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
func (cg *ConsumerGroupInfo) doChannelCloseEvent(remoteAddr string, conn net.Conn) bool {
	info, err := cg.ConnTable.Remove(conn)
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
func (cg *ConsumerGroupInfo) getAllChannel() []net.Conn {
	result := []net.Conn{}
	iterator := cg.ConnTable.Iterator()
	for iterator.HasNext() {
		key, _, _ := iterator.Next()
		if channel, ok := key.(net.Conn); ok {
			result = append(result, channel)
		}
	}
	return result
}

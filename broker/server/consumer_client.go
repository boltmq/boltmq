// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package server

import (
	"fmt"
	"strings"
	"time"

	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/net/core"
	"github.com/boltmq/common/protocol/heartbeat"
	"github.com/boltmq/common/utils/system"
	set "github.com/deckarep/golang-set"
	concurrent "github.com/fanliao/go-concurrentMap"
)

type ConsumerIdsChangeListener interface {
	ConsumerIdsChanged(group string, channels []core.Context)
}

// defaultConsumerIdsChangeListener ConsumerId列表变化，通知所有Consumer
// Author gaoyanlei
// Since 2017/8/9
type defaultConsumerIdsChangeListener struct {
	brokerController *BrokerController
}

// newDefaultConsumerIdsChangeListener 初始化
// Author gaoyanlei
// Since 2017/8/9
func newDefaultConsumerIdsChangeListener(brokerController *BrokerController) *defaultConsumerIdsChangeListener {
	var listener = new(defaultConsumerIdsChangeListener)
	listener.brokerController = brokerController
	return listener
}

// ConsumerIdsChanged 通知Consumer改变
// Author gaoyanlei
// Since 2017/8/9
func (listener *defaultConsumerIdsChangeListener) ConsumerIdsChanged(group string, channels []core.Context) {
	if channels != nil && listener.brokerController.cfg.Broker.NotifyConsumerIdsChangedEnable {
		for _, conn := range channels {
			listener.brokerController.b2Client.notifyConsumerIdsChanged(conn, group)
		}
	}
}

// consumerGroupInfo 整个Consumer Group信息
// Author gaoyanlei
// Since 2017/8/17
type consumerGroupInfo struct {
	groupName           string
	subscriptionTable   *concurrent.ConcurrentMap // key:Topic, val:SubscriptionDataPlus
	connTable           *concurrent.ConcurrentMap // key: Channel.Addr() val: channelInfo
	consumeType         heartbeat.ConsumeType
	msgModel            heartbeat.MessageModel
	consumeFromWhere    heartbeat.ConsumeFromWhere
	lastUpdateTimestamp int64
}

func newConsumerGroupInfo(groupName string, consumeType heartbeat.ConsumeType, messageModel heartbeat.MessageModel,
	consumeFromWhere heartbeat.ConsumeFromWhere) *consumerGroupInfo {
	var cg = new(consumerGroupInfo)
	cg.subscriptionTable = concurrent.NewConcurrentMap()
	cg.connTable = concurrent.NewConcurrentMap()
	cg.groupName = groupName
	cg.consumeType = consumeType
	cg.msgModel = messageModel
	cg.consumeFromWhere = consumeFromWhere
	return cg
}

func (cg *consumerGroupInfo) findSubscriptionDataPlus(topic string) *heartbeat.SubscriptionDataPlus {
	value, err := cg.subscriptionTable.Get(topic)
	if err != nil || value == nil {
		return nil
	}
	if subscriptionData, ok := value.(*heartbeat.SubscriptionDataPlus); ok {
		return subscriptionData
	}
	return nil
}

func (cg *consumerGroupInfo) findSubscriptionData(topic string) *heartbeat.SubscriptionData {
	value, err := cg.subscriptionTable.Get(topic)
	if err != nil || value == nil {
		return nil
	}
	if sdPlus, ok := value.(*heartbeat.SubscriptionDataPlus); ok {
		subscriptionData := &heartbeat.SubscriptionData{
			Topic:           sdPlus.Topic,
			SubAll:          sdPlus.SubAll,
			SubString:       sdPlus.SubString,
			SubVersion:      sdPlus.SubVersion,
			TagsSet:         set.NewSet(sdPlus.TagsSet),
			CodeSet:         set.NewSet(sdPlus.CodeSet),
			ClassFilterMode: sdPlus.ClassFilterMode,
		}
		return subscriptionData
	}
	return nil
}

/**
 * updateChannel 更新通道
 * Author gaoyanlei
 * Since 2017/9/21
 */
func (cg *consumerGroupInfo) updateChannel(infoNew *channelInfo, consumeType heartbeat.ConsumeType,
	messageModel heartbeat.MessageModel, consumeFromWhere heartbeat.ConsumeFromWhere) bool {
	updated := false
	cg.consumeType = consumeType
	cg.msgModel = messageModel
	cg.consumeFromWhere = consumeFromWhere
	infoOld, err := cg.connTable.Get(infoNew.ctx.UniqueSocketAddr().String())
	if infoOld == nil || err != nil {
		prev, err := cg.connTable.Put(infoNew.ctx.UniqueSocketAddr().String(), infoNew)
		if prev == nil || err != nil {
			logger.Infof("new consumer connected, group: %s, consumeType:%v, messageModel:%v, channel: %s",
				cg.groupName, consumeType, messageModel, infoNew.ctx.UniqueSocketAddr())
			updated = true
		}
		infoOld = infoNew
	} else {
		if info, ok := infoOld.(*channelInfo); ok {
			if !strings.EqualFold(infoNew.clientId, info.clientId) {
				logger.Errorf("[BUG] consumer channel exist in broker, but clientId not equal. GROUP: %s OLD: %s NEW: %s.",
					cg.groupName, info, infoNew)
				cg.connTable.Put(infoNew.ctx.UniqueSocketAddr().String(), infoNew)
			}
		}
	}

	cg.lastUpdateTimestamp = system.CurrentTimeMillis()
	if infoOld, ok := infoOld.(*channelInfo); ok {
		infoOld.lastUpdateTimestamp = cg.lastUpdateTimestamp
	}

	return updated
}

// doChannelCloseEvent 关闭通道
// Author rongzhihong
// Since 2017/9/11
func (cg *consumerGroupInfo) doChannelCloseEvent(remoteAddr string, ctx core.Context) bool {
	if cg.connTable.Size() <= 0 {
		return false
	}

	info, err := cg.connTable.Remove(ctx.UniqueSocketAddr().String())
	if err != nil {
		logger.Errorf("doChannel close event err: %s.", err)
		return false
	}
	if info != nil {
		logger.Warnf("remove not active channel[%s] from consumer groupInfo groupChannelTable, consumer group: %s.",
			ctx.UniqueSocketAddr(), cg.groupName)
		return true
	}
	return false
}

// getAllChannel 获得所有通道
// Author rongzhihong
// Since 2017/9/11
func (cg *consumerGroupInfo) getAllChannel() []core.Context {
	result := []core.Context{}
	for iterator := cg.connTable.Iterator(); iterator.HasNext(); {
		_, value, _ := iterator.Next()
		if channel, ok := value.(*channelInfo); ok {
			result = append(result, channel.ctx)
		}
	}
	return result
}

// getAllClientId 获得所有客户端ID
// Author rongzhihong
// Since 2017/9/14
func (cg *consumerGroupInfo) getAllClientId() []string {
	result := []string{}
	for iterator := cg.connTable.Iterator(); iterator.HasNext(); {
		_, value, _ := iterator.Next()
		if channel, ok := value.(*channelInfo); ok {
			result = append(result, channel.clientId)
		}
	}
	return result
}

// unregisterChannel 注销通道
// Author rongzhihong
// Since 2017/9/14
func (cg *consumerGroupInfo) unregisterChannel(chanInfo *channelInfo) {
	if cg.connTable.Size() <= 0 {
		return
	}

	old, _ := cg.connTable.Remove(chanInfo.ctx.UniqueSocketAddr().String())
	if old != nil {
		logger.Infof("unregister a consumer[%s] from consumerGroupInfo %v.", cg.groupName, old)
	}
}

// updateSubscription 更新订阅
// Author rongzhihong
// Since 2017/9/17
func (cg *consumerGroupInfo) updateSubscription(subList []heartbeat.SubscriptionDataPlus) bool {
	updated := false

	// 增加新的订阅关系
	for _, sub := range subList {
		old, _ := cg.subscriptionTable.Get(sub.Topic)
		if old == nil {
			prev, _ := cg.subscriptionTable.Put(sub.Topic, &sub)
			if prev == nil {
				updated = true
				logger.Infof("subscription changed, add new topic, group: %s %#v.", cg.groupName, sub)
			}
			continue
		}

		if oldSub, ok := old.(*heartbeat.SubscriptionDataPlus); ok {
			if sub.SubVersion > oldSub.SubVersion {
				if cg.consumeType == heartbeat.CONSUME_PASSIVELY {
					logger.Infof("subscription changed, group: %s OLD: %#v NEW: %#v.", cg.groupName, oldSub, sub)
				}
				cg.subscriptionTable.Put(sub.Topic, &sub)
			}
		}
	}

	// 删除老的订阅关系
	for subIt := cg.subscriptionTable.Iterator(); subIt.HasNext(); {
		exist := false
		oldTopic, oldValue, _ := subIt.Next()
		for _, subItem := range subList {
			if oldTopic, ok := oldTopic.(string); ok && strings.EqualFold(subItem.Topic, oldTopic) {
				exist = true
				break
			}
		}

		if !exist {
			logger.Warnf("subscription changed, group: %s remove topic %s %v.", cg.groupName, oldTopic, oldValue)
			subIt.Remove()
			updated = true
		}
	}

	cg.lastUpdateTimestamp = system.CurrentTimeMillis()
	return updated
}

// findChannel 根据clientId获得通道
// Author rongzhihong
// Since 2017/9/17
func (cg *consumerGroupInfo) findChannel(clientId string) *channelInfo {
	for iterator := cg.connTable.Iterator(); iterator.HasNext(); {
		_, value, _ := iterator.Next()
		if info, ok := value.(*channelInfo); ok {
			if strings.EqualFold(info.clientId, clientId) {
				return info
			}
		}
	}
	return nil
}

// subscriptionTableToMap subscriptionTable To Map
// Author rongzhihong
// Since 2017/9/17
func (cg *consumerGroupInfo) subscriptionTableToMap() map[string]*heartbeat.SubscriptionDataPlus {
	subscriptionDataMap := make(map[string]*heartbeat.SubscriptionDataPlus)
	for iterator := cg.subscriptionTable.Iterator(); iterator.HasNext(); {
		k, v, _ := iterator.Next()
		subscriptionDataMap[k.(string)] = v.(*heartbeat.SubscriptionDataPlus)
	}
	return subscriptionDataMap
}

type channelInfo struct {
	ctx                 core.Context
	clientId            string
	langCode            string
	addr                string
	version             int32
	lastUpdateTimestamp int64
}

func newChannelInfo(ctx core.Context, clientId string, langCode, addr string, version int32) *channelInfo {
	var channelInfo = new(channelInfo)
	channelInfo.ctx = ctx
	channelInfo.clientId = clientId
	channelInfo.langCode = langCode
	channelInfo.addr = addr
	channelInfo.version = version
	channelInfo.lastUpdateTimestamp = time.Now().Unix() * 1000
	return channelInfo
}

func (info *channelInfo) String() string {
	format := "client channel info {%s, clientId=%s, language=%s, version=%d, lastUpdateTimestamp=%d }"
	return fmt.Sprintf(format, info.ctx.String(), info.clientId, info.langCode, info.version, info.lastUpdateTimestamp)
}

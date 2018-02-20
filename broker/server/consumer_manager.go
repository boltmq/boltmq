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
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/net/core"
	"github.com/boltmq/common/protocol/heartbeat"
	"github.com/boltmq/common/utils/system"
	set "github.com/deckarep/golang-set"
	concurrent "github.com/fanliao/go-concurrentMap"
)

// consumerManager 消费者管理
// Author gaoyanlei
// Since 2017/8/9
type consumerManager struct {
	consumerTable         *concurrent.ConcurrentMap // key:group, value:ConsumerGroupInfo
	cisListener           ConsumerIdsChangeListener
	channelExpiredTimeout int64
}

// newConsumerManager 初始化ConsumerManager
// Author gaoyanlei
// Since 2017/8/9
func newConsumerManager(cisListener ConsumerIdsChangeListener) *consumerManager {
	var cm = new(consumerManager)
	cm.consumerTable = concurrent.NewConcurrentMap()
	cm.cisListener = cisListener
	cm.channelExpiredTimeout = 1000 * 120
	return cm
}

func (cm *consumerManager) getConsumerGroupInfo(group string) *consumerGroupInfo {
	value, err := cm.consumerTable.Get(group)
	if err != nil {
		return nil
	}

	if cgi, ok := value.(*consumerGroupInfo); ok {
		return cgi
	}

	return nil
}

func (cm *consumerManager) findSubscriptionData(group, topic string) *heartbeat.SubscriptionDataPlus {
	cgi := cm.getConsumerGroupInfo(group)
	if cgi != nil {
		return cgi.findSubscriptionDataPlus(topic)
	}
	return nil
}

// registerConsumer 注册Consumer
// Author gaoyanlei
// Since 2017/8/24
func (cm *consumerManager) registerConsumer(group string, chanInfo *channelInfo, consumeType heartbeat.ConsumeType,
	messageModel heartbeat.MessageModel, consumeFromWhere heartbeat.ConsumeFromWhere, subList []heartbeat.SubscriptionDataPlus) bool {
	cgi := cm.getConsumerGroupInfo(group)
	if nil == cgi {
		tmp := newConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere)
		prev, err := cm.consumerTable.PutIfAbsent(group, tmp)
		if err != nil || prev == nil {
			cgi = tmp
		} else {
			if info, ok := prev.(*consumerGroupInfo); ok {
				cgi = info
			}
		}
	}

	r1 := cgi.updateChannel(chanInfo, consumeType, messageModel, consumeFromWhere)
	r2 := cgi.updateSubscription(subList)

	if r1 || r2 {
		cm.cisListener.ConsumerIdsChanged(group, cgi.getAllChannel())
	}

	return r1 || r2
}

// unregisterConsumer 注销消费者
// Author rongzhihong
// Since 2017/9/18
func (cm *consumerManager) unregisterConsumer(group string, chanInfo *channelInfo) {
	cgi, _ := cm.consumerTable.Get(group)
	if cgi != nil {
		if info, ok := cgi.(*consumerGroupInfo); ok {
			info.unregisterChannel(chanInfo)
			if info.connTable.IsEmpty() {
				removeOld, _ := cm.consumerTable.Remove(group)
				if removeOld != nil {
					logger.Infof("unRegister consumer ok, no any connection, and remove consumer group, %s.", group)
				}
			}
			cm.cisListener.ConsumerIdsChanged(group, info.getAllChannel())
		}
	}
}

// scanNotActiveChannel 扫描不活跃的通道
// Author rongzhihong
// Since 2017/9/11
func (cm *consumerManager) scanNotActiveChannel() {
	for iterator := cm.consumerTable.Iterator(); iterator.HasNext(); {
		key, value, _ := iterator.Next()
		group, ok := key.(string)
		if !ok {
			continue
		}
		cgi, ok := value.(*consumerGroupInfo)
		if !ok {
			continue
		}
		channelInfoTable := cgi.connTable
		chanIterator := channelInfoTable.Iterator()
		for chanIterator.HasNext() {
			_, clientValue, _ := chanIterator.Next()
			chanInfo, ok := clientValue.(*channelInfo)
			if !ok {
				continue
			}
			diff := system.CurrentTimeMillis() - chanInfo.lastUpdateTimestamp
			if diff > cm.channelExpiredTimeout {
				logger.Warnf("SCAN: remove expired channel from consumerManager consumerTable. channel=%s, consumerGroup=%s.",
					chanInfo.addr, group)
				chanInfo.ctx.Close()
				chanIterator.Remove()
			}
		}

		if channelInfoTable.IsEmpty() {
			logger.Warnf("SCAN: remove expired channel from consumerManager consumerTable, all clear, consumerGroup=%s.", group)
			iterator.Remove()
		}
	}
}

// doChannelCloseEvent 扫描不活跃的通道
// Author rongzhihong
// Since 2017/9/11
func (cm *consumerManager) doChannelCloseEvent(remoteAddr string, ctx core.Context) {
	iterator := cm.consumerTable.Iterator()
	for iterator.HasNext() {
		key, value, _ := iterator.Next()
		group, ok := key.(string)
		if !ok {
			continue
		}
		cgi, ok := value.(*consumerGroupInfo)
		if !ok {
			continue
		}
		isRemoved := cgi.doChannelCloseEvent(remoteAddr, ctx)
		if isRemoved {
			if cgi.connTable.IsEmpty() {
				remove, err := cm.consumerTable.Remove(group)
				if err != nil {
					logger.Error("doChannel close event err: %s.", err)
					continue
				}
				if remove != nil {
					logger.Infof("unRegister consumer ok, no any connection, and remove consumer group, %s.", group)
				}
			}
			cm.cisListener.ConsumerIdsChanged(group, cgi.getAllChannel())
		}
	}
}

// findSubscriptionDataCount 根据group查找订阅数量
// Author rongzhihong
// Since 2017/9/18
func (cm *consumerManager) findSubscriptionDataCount(group string) int32 {
	cgi, _ := cm.consumerTable.Get(group)
	if cgi != nil {
		if info, ok := cgi.(*consumerGroupInfo); ok {
			return info.subscriptionTable.Size()
		}
	}
	return 0
}

// queryTopicConsumeByWho 根据topic查找消费者
// Author rongzhihong
// Since 2017/9/18
func (cm *consumerManager) queryTopicConsumeByWho(topic string) set.Set {
	groups := set.NewSet()
	iterator := cm.consumerTable.Iterator()
	for iterator.HasNext() {
		group, value, _ := iterator.Next()
		if info, ok := value.(*consumerGroupInfo); ok {
			subscriptionTable := info.subscriptionTable
			if found, _ := subscriptionTable.Get(topic); found != nil {
				if k, ok := group.(string); ok {
					groups.Add(k)
				}
			}
		}
	}
	return groups
}

// findChannel 获得某个组某个消费Id对应的通道
// Author rongzhihong
// Since 2017/9/18
func (cm *consumerManager) findChannel(group, clientId string) *channelInfo {
	cgi, err := cm.consumerTable.Get(group)
	if err != nil {
		return nil
	}

	if cgi != nil {
		if info, ok := cgi.(*consumerGroupInfo); ok {
			return info.findChannel(clientId)
		}
	}

	return nil
}

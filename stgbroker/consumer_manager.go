package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/client"
	"git.oschina.net/cloudzone/smartgo/stgbroker/client/rebalance"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	set "github.com/deckarep/golang-set"
	"net"
)

// ConsumerManager 消费者管理
// Author gaoyanlei
// Since 2017/8/9
type ConsumerManager struct {
	consumerTable             *sync.Map
	ConsumerIdsChangeListener rebalance.ConsumerIdsChangeListener
	ChannelExpiredTimeout     int64
}

// NewConsumerOffsetManager 初始化ConsumerOffsetManager
// Author gaoyanlei
// Since 2017/8/9
func NewConsumerManager(consumerIdsChangeListener rebalance.ConsumerIdsChangeListener) *ConsumerManager {
	var consumerManager = new(ConsumerManager)
	consumerManager.consumerTable = sync.NewMap()
	consumerManager.ConsumerIdsChangeListener = consumerIdsChangeListener
	consumerManager.ChannelExpiredTimeout = 1000 * 120
	return consumerManager
}

func (cm *ConsumerManager) getConsumerGroupInfo(group string) *client.ConsumerGroupInfo {
	value, err := cm.consumerTable.Get(group)
	if err != nil {
		return nil
	}

	if consumerGroupInfo, ok := value.(*client.ConsumerGroupInfo); ok {
		return consumerGroupInfo
	}

	return nil
}

func (cm *ConsumerManager) FindSubscriptionData(group, topic string) *heartbeat.SubscriptionData {
	consumerGroupInfo := cm.getConsumerGroupInfo(group)
	if consumerGroupInfo != nil {
		return consumerGroupInfo.FindSubscriptionData(topic)
	}
	return nil
}

func (cm *ConsumerManager) registerConsumer(group string, conn net.Conn, consumeType heartbeat.ConsumeType,
	messageModel heartbeat.MessageModel, consumeFromWhere heartbeat.ConsumeFromWhere, subList set.Set) bool {
	consumerGroupInfo := cm.getConsumerGroupInfo(group)
	if nil == consumerGroupInfo {
		tmp := client.NewConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere)
		prev, err := cm.consumerTable.Put(group, tmp)
		if err != nil || prev == nil {
			consumerGroupInfo = tmp
		} else {
			if consumerGroupInfo, ok := prev.(*client.ConsumerGroupInfo); ok {
				consumerGroupInfo = consumerGroupInfo
			}
		}
	}
	r1 := consumerGroupInfo.UpdateChannel(conn, consumeType, messageModel, consumeFromWhere)
	// TODO
	return r1
}

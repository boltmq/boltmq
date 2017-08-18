package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/client"
	"git.oschina.net/cloudzone/smartgo/stgbroker/client/rebalance"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
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

func (self *ConsumerManager) getConsumerGroupInfo(group string) *client.ConsumerGroupInfo {
	value, err := self.consumerTable.Get(group)
	if err != nil {
		return nil
	}

	if consumerGroupInfo, ok := value.(*client.ConsumerGroupInfo); ok {
		return consumerGroupInfo
	}

	return nil
}

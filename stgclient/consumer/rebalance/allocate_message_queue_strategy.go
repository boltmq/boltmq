package rebalance

import "container/list"

// AllocateMessageQueueStrategy 消费负载策略接口
// Author: yintongqiang
// Since:  2017/8/8

type AllocateMessageQueueStrategy interface {
	// Allocating by consumer id
	// consumerGroup current consumer group
	// currentCID    current consumer id
	// mqAll         message queue set in current topic
	// cidAll        consumer set in current consumer group
	Allocate(
	consumerGroup  string,
	currentCID string,
	mqAll *list.List,
	cidAll *list.List) *list.List
	// Algorithm name
	GetName() string
}

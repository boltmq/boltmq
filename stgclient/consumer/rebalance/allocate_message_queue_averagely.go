package rebalance

import "container/list"


// 平均负载
// Author: yintongqiang
// Since:  2017/8/8

type AllocateMessageQueueAveragely struct {

}

func (strategy AllocateMessageQueueAveragely) Allocate(consumerGroup  string, currentCID string, mqAll *list.List, cidAll *list.List) *list.List {
	result := list.New()
	return result
}

func (strategy AllocateMessageQueueAveragely) GetName() string {
	return "AVG"
}

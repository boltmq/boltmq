package rebalance

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)


// 平均负载
// Author: yintongqiang
// Since:  2017/8/8

type AllocateMessageQueueAveragely struct {

}

func (strategy AllocateMessageQueueAveragely) Allocate(consumerGroup string, currentCID string, mqAll [] message.MessageQueue, cidAll [] string) [] message.MessageQueue {

	return []message.MessageQueue{}
}

func (strategy AllocateMessageQueueAveragely) GetName() string {
	return "AVG"
}

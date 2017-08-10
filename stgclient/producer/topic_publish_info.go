package producer

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"strings"
	//"sync/atomic"
	//"math"
	"sync/atomic"
	"math"
)

type TopicPublishInfo struct {
	OrderTopic          bool
	HaveTopicRouterInfo bool
	MessageQueueList    []*message.MessageQueue
	SendWhichQueue      int64
}

func NewTopicPublishInfo() *TopicPublishInfo {
	return &TopicPublishInfo{}
}

func (topicPublishInfo *TopicPublishInfo)SelectOneMessageQueue(lastBrokerName string) *message.MessageQueue {
	index := atomic.AddInt64(&topicPublishInfo.SendWhichQueue, 1)
	if !strings.EqualFold(lastBrokerName, "") {
		for _, mq := range topicPublishInfo.MessageQueueList {
			index++
			pos := int(math.Abs(float64(index))) % len(topicPublishInfo.MessageQueueList)
			mq = topicPublishInfo.MessageQueueList[pos]
			if !strings.EqualFold(mq.BrokerName, lastBrokerName) {
				return mq;
			}
		}
		return nil
	} else {
		pos := int(math.Abs(float64(index))) % len(topicPublishInfo.MessageQueueList)
		mq := topicPublishInfo.MessageQueueList[pos]
		return mq
	}
}


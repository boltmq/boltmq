package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"strings"
	//"sync/atomic"
	//"math"
	"fmt"
	"math"
	"sync/atomic"
)

// TopicPublishInfo: topic发布信息
// Author: yintongqiang
// Since:  2017/8/10
type TopicPublishInfo struct {
	OrderTopic          bool
	HaveTopicRouterInfo bool
	MessageQueueList    []*message.MessageQueue
	SendWhichQueue      int64
}

func NewTopicPublishInfo() *TopicPublishInfo {
	return &TopicPublishInfo{}
}

// 取模获取选择队列
func (topicPublishInfo *TopicPublishInfo) SelectOneMessageQueue(lastBrokerName string) *message.MessageQueue {
	if !strings.EqualFold(lastBrokerName, "") {
		index := topicPublishInfo.SendWhichQueue
		atomic.AddInt64(&topicPublishInfo.SendWhichQueue, 1)
		for _, mq := range topicPublishInfo.MessageQueueList {
			index++
			value := int(math.Abs(float64(index)))
			pos := value % len(topicPublishInfo.MessageQueueList)
			mq = topicPublishInfo.MessageQueueList[pos]
			if !strings.EqualFold(mq.BrokerName, lastBrokerName) {
				return mq
			}
		}
		return nil
	} else {
		index := topicPublishInfo.SendWhichQueue
		atomic.AddInt64(&topicPublishInfo.SendWhichQueue, 1)
		value := int(math.Abs(float64(index)))
		pos := value % len(topicPublishInfo.MessageQueueList)
		mq := topicPublishInfo.MessageQueueList[pos]
		return mq
	}
}

func (self *TopicPublishInfo) ToString() string {
	if self == nil {
		return ""
	}

	messageQueueData := ""
	if self.MessageQueueList != nil && len(self.MessageQueueList) > 0 {
		values := []string{}
		for _, mq := range self.MessageQueueList {
			format := "{topic=%s, brokerName=%s, queueId=%d}"
			values = append(values, fmt.Sprintf(format, mq.Topic, mq.BrokerName, mq.QueueId))
		}
		messageQueueData = strings.Join(values, ",")
	}

	format := "TopicPublishInfo [orderTopic=%t, messageQueueList=[%s], sendWhichQueue=%d, haveTopicRouterInfo=%t]"
	return fmt.Sprintf(format, self.OrderTopic, messageQueueData, self.SendWhichQueue, self.HaveTopicRouterInfo)
}

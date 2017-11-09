package message

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"strconv"
	"strings"
)

type MessageQueue struct {
	Topic      string `json:"topic"`
	BrokerName string `json:"brokerName"`
	QueueId    int    `json:"queueId"`
}

func NewMessageQueue() *MessageQueue {
	messageQueue := new(MessageQueue)
	return messageQueue
}

func NewDefaultMessageQueue(topic, brokerName string, queueId int) *MessageQueue {
	messageQueue := &MessageQueue{
		Topic:      topic,
		BrokerName: brokerName,
		QueueId:    queueId,
	}
	return messageQueue
}

func (mq MessageQueue) HashBytes() []byte {
	return []byte(strconv.Itoa(mq.hashCode()))
}

func (mq MessageQueue) Equals(v interface{}) bool {
	if v == nil {
		return false
	}
	mq1, ok := v.(MessageQueue)
	var mq2 *MessageQueue
	if !ok {
		mq2, ok = v.(*MessageQueue)
	}
	if mq2 == nil {
		return ok && (strings.EqualFold(mq.BrokerName, mq1.BrokerName) && strings.EqualFold(mq.Topic, mq1.Topic) && mq.QueueId == mq1.QueueId)
	} else {
		return ok && (strings.EqualFold(mq.BrokerName, mq2.BrokerName) && strings.EqualFold(mq.Topic, mq2.Topic) && mq.QueueId == mq2.QueueId)
	}
}

func (mq MessageQueue) hashCode() int {
	var prime int = 31
	var result int = 1
	if strings.EqualFold(mq.BrokerName, "") {
		result = prime*result + 0
	} else {
		result = prime*result + int(stgcommon.HashCode(mq.BrokerName))
	}
	result = prime*result + mq.QueueId
	if strings.EqualFold(mq.Topic, "") {
		result = prime*result + 0
	} else {
		result = prime*result + int(stgcommon.HashCode(mq.Topic))
	}
	return result
}

type MessageQueues []*MessageQueue

func (self MessageQueues) Less(i, j int) bool {
	imq := self[i]
	jmq := self[j]

	if imq.Topic < jmq.Topic {
		return true
	} else if imq.Topic > jmq.Topic {
		return false
	}

	if imq.BrokerName < jmq.BrokerName {
		return true
	} else if imq.BrokerName > jmq.BrokerName {
		return false
	}

	if imq.QueueId < jmq.QueueId {
		return true
	} else {
		return false
	}
}

func (self MessageQueues) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self MessageQueues) Len() int {
	return len(self)
}

func (mq MessageQueue) ToString() string {
	format := "MessageQueue {topic=%s, brokerName=%s, queueId=%d}"
	return fmt.Sprintf(format, mq.Topic, mq.BrokerName, mq.QueueId)
}

func (mq MessageQueue) Key() string {
	return fmt.Sprintf("%s@%s@%d", mq.Topic, mq.BrokerName, mq.QueueId)
}

func (mq MessageQueue) Equal(other MessageQueue) bool {
	return strings.EqualFold(other.Topic, mq.Topic) && strings.EqualFold(other.BrokerName, mq.BrokerName) && other.QueueId == mq.QueueId
}

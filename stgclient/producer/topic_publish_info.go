package producer

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)

type TopicPublishInfo struct {
	OrderTopic          bool
	HaveTopicRouterInfo bool
	MessageQueueList    []message.MessageQueue
	SendWhichQueue      int64
}

func NewTopicPublishInfo() *TopicPublishInfo {
	return &TopicPublishInfo{}
}
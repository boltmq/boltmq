package header

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
)

// CreateTopicRequestHeader: 创建topic头信息
// Author: yintongqiang
// Since:  2017/8/17
type CreateTopicRequestHeader struct {
	Topic           string // 真正的topic名称是位于topicConfig.Topic字段
	DefaultTopic    string // 表示创建topic的key值
	ReadQueueNums   int32
	WriteQueueNums  int32
	Perm            int
	TopicFilterType stgcommon.TopicFilterType
	TopicSysFlag    int
	Order           bool
}

func (header *CreateTopicRequestHeader) CheckFields() error {
	return nil
}

func NewCreateTopicRequestHeader(topicWithProjectGroup, defaultTopic string, topicConfig *stgcommon.TopicConfig) *CreateTopicRequestHeader {
	createTopicRequestHeader := &CreateTopicRequestHeader{
		Topic:           topicWithProjectGroup,
		DefaultTopic:    defaultTopic,
		ReadQueueNums:   topicConfig.ReadQueueNums,
		WriteQueueNums:  topicConfig.WriteQueueNums,
		TopicFilterType: topicConfig.TopicFilterType,
		TopicSysFlag:    topicConfig.TopicSysFlag,
		Order:           topicConfig.Order,
		Perm:            topicConfig.Perm,
	}
	return createTopicRequestHeader
}

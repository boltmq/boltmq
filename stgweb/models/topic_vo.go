package models

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/admin"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"strings"
)

const (
	NORMAL_TOPIC TopicType = iota // 正常队列的Topic
	RETRY_TOPIC                   // 重试队列Topic
	DLQ_TOPIC                     // 死信队列Topic
)

// TopicType topic类型
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
type TopicType int

// ToString 转化为字符串
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (topicType TopicType) ToString() string {
	switch topicType {
	case NORMAL_TOPIC:
		return "NORMAL_TOPIC"
	case RETRY_TOPIC:
		return "RETRY_TOPIC"
	case DLQ_TOPIC:
		return "DLQ_TOPIC"
	default:
		return "Unknown"
	}
}

// ParseTopicType 转化为topic类型
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func ParseTopicType(topic string) TopicType {
	if IsNormalTopic(topic) {
		return NORMAL_TOPIC
	}
	if IsRetryTopic(topic) {
		return RETRY_TOPIC
	}
	if IsDLQTopic(topic) {
		return DLQ_TOPIC
	}
	return NORMAL_TOPIC
}

// TopicStats topic数据的存储状态
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
type TopicStats struct {
	TopicType      TopicType `json:"topicType"`      // topic类型
	Topic          string    `json:"topic"`          // topic名称
	BrokerName     string    `json:"brokerName"`     // broker名称
	ClusterName    string    `json:"clusterName"`    // 集群名称
	QueueID        int       `json:"queueId"`        // 队列ID
	MinOffset      int64     `json:"minOffset"`      // 最小偏移量
	MaxOffset      int64     `json:"maxOffset"`      // 最大偏移量
	LastUpdateTime string    `json:"lastUpdateTime"` // 最近更新时间
}

// ToTopicStats 转化为topic状态
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func ToTopicStats(mq *message.MessageQueue, topicOffset *admin.TopicOffset, topic, clusterName string) *TopicStats {
	topicStats := &TopicStats{
		Topic:       topic,
		ClusterName: clusterName,
		BrokerName:  mq.BrokerName,
		QueueID:     mq.QueueId,
		MinOffset:   topicOffset.MinOffset,
		MaxOffset:   topicOffset.MaxOffset,
	}

	humanTimestamp := ""
	if topicOffset.LastUpdateTimestamp > 0 {
		humanTimestamp = stgcommon.FormatTimestamp(topicOffset.LastUpdateTimestamp)
	}
	topicStats.LastUpdateTime = humanTimestamp
	topicStats.TopicType = ParseTopicType(topic)

	return topicStats
}

// IsRetryTopic 是否为重试队列的Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func IsRetryTopic(topic string) bool {
	return strings.HasPrefix(strings.TrimSpace(topic), stgcommon.RETRY_GROUP_TOPIC_PREFIX)
}

// IsDLQTopic 是否为死信队列的Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func IsDLQTopic(topic string) bool {
	return strings.HasPrefix(strings.TrimSpace(topic), stgcommon.DLQ_GROUP_TOPIC_PREFIX)
}

// IsNormalTopic 是否正常的Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func IsNormalTopic(topic string) bool {
	return !IsRetryTopic(topic) && !IsDLQTopic(topic)
}

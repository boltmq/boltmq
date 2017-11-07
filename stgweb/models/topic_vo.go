package models

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/admin"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"github.com/gunsluo/govalidator"
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
	TopicClusterCommon
	TopicType      TopicType `json:"topicType"`      // topic类型
	BrokerName     string    `json:"brokerName"`     // broker名称
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
		BrokerName: mq.BrokerName,
		QueueID:    mq.QueueId,
		MinOffset:  topicOffset.MinOffset,
		MaxOffset:  topicOffset.MaxOffset,
	}

	humanTimestamp := ""
	if topicOffset.LastUpdateTimestamp > 0 {
		humanTimestamp = stgcommon.FormatTimestamp(topicOffset.LastUpdateTimestamp)
	}
	topicStats.LastUpdateTime = humanTimestamp

	topicStats.TopicType = ParseTopicType(topic)
	topicStats.Topic = topic
	topicStats.ClusterName = clusterName

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

// DeleteTopic 删除Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type DeleteTopic struct {
	TopicClusterCommon
}

// UpdateTopic 删除Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type UpdateTopic struct {
	TopicCommon
	TopicClusterCommon
}

// CreateTopic 创建Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type CreateTopic struct {
	ClusterName string `json:"clusterName" valid:"required"` // 集群名称
	Topic       string `json:"topic" valid:"required"`       // topic名称
}

// Validate 参数验证
func (createTopic *CreateTopic) Validate() error {
	if err := utils.ValidateStruct(createTopic); err != nil {
		return createTopic.customizeValidationErr(err)
	}
	return nil
}

// 自定义错误提示
func (createTopic *CreateTopic) customizeValidationErr(err error) error {
	if _, ok := err.(*govalidator.UnsupportedTypeError); ok {
		return nil
	}

	for _, ve := range err.(govalidator.Errors) {
		e, ok := ve.(govalidator.Error)
		if !ok {
			continue
		}
		switch e.Name {
		case "clusterName":
			return fmt.Errorf("集群名称'%s'字段无效", e.Name)
		case "topic":
			return fmt.Errorf("topic名称'%s'字段无效", e.Name)
		}
	}

	return err
}

type TopicListVo struct {
	TopicCommon TopicCommon `json:"topicConfig"`
	TopicClusterCommon
}

// CreateTopic 创建Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type TopicCommon struct {
	BrokerAddr      string                    `json:"brokerAddr"`      // broker地址
	WriteQueueNums  int                       `json:"writeQueueNums"`  // 写队列数
	Unit            bool                      `json:"unit"`            // 是否为单元topic
	ReadQueueNums   int                       `json:"readQueueNums"`   // 读队列数
	Order           bool                      `json:"order"`           // 是否为顺序topic
	Perm            int                       `json:"perm"`            // 对应的broker读写权限
	TopicFilterType stgcommon.TopicFilterType `json:"topicFilterType"` // topic过滤类型
	TopicSysFlag    int                       `json:"topicSysFlag"`    // 是否为系统topic标记
}

// TopicClusterCommon topic与cluster公共配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type TopicClusterCommon struct {
	ClusterName string    `json:"clusterName"` // 集群名称
	Topic       string    `json:"topic"`       // topic名称
	TopicType   TopicType `json:"topicType"`   // topic类型
}

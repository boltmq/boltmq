package models

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/admin"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"strings"
)

const (
	ALL_TOPIC    TopicType = iota // 查询所有Topic
	NORMAL_TOPIC                  // 正常队列的Topic
	RETRY_TOPIC                   // 重试队列Topic
	DLQ_TOPIC                     // 死信队列Topic
)

// TopicType topic类型
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
type TopicType int

// SystemTopics 默认系统Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
var SystemTopics = []string{
	stgcommon.SELF_TEST_TOPIC,
	stgcommon.DEFAULT_TOPIC,
	stgcommon.BENCHMARK_TOPIC,
	stgcommon.OFFSET_MOVED_EVENT,
	stgcommon.GetDefaultBrokerName(),
}

// TopicStats topic数据的存储状态
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
type TopicStats struct {
	ClusterName    string    `json:"clusterName"`    // 集群名称
	Topic          string    `json:"topic"`          // topic名称
	TopicType      TopicType `json:"topicType"`      // topic类型
	BrokerName     string    `json:"brokerName"`     // broker名称
	QueueID        int       `json:"queueId"`        // 队列ID
	MinOffset      int64     `json:"minOffset"`      // 最小偏移量
	MaxOffset      int64     `json:"maxOffset"`      // 最大偏移量
	LastUpdateTime string    `json:"lastUpdateTime"` // 最近更新时间
}

// DeleteTopic 删除Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type DeleteTopic struct {
	ClusterName string `json:"clusterName"` // 集群名称
	Topic       string `json:"topic"`       // topic名称
}

// UpdateTopic 删除Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type UpdateTopic struct {
	ClusterName    string `json:"clusterName" valid:"required"` // 集群名称
	Topic          string `json:"topic" valid:"required"`       // topic名称
	BrokerAddr     string `json:"brokerAddr" valid:"required"`  // broker地址
	WriteQueueNums int    `json:"writeQueueNums" valid:"min=8"` // 写队列数
	ReadQueueNums  int    `json:"readQueueNums" valid:"min=8"`  // 读队列数
	Unit           bool   `json:"unit"`                         // 是否为单元topic
	Order          bool   `json:"order"`                        // 是否为顺序topic
}

// CreateTopic 创建Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type CreateTopic struct {
	ClusterName string `json:"clusterName" valid:"required"` // 集群名称
	Topic       string `json:"topic" valid:"required"`       // topic名称
}

// TopicVo 查询Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
type TopicVo struct {
	TopicConfigVo *TopicConfigVo `json:"topicConfig"`
	ClusterName   string         `json:"clusterName"`   // 集群名称
	Topic         string         `json:"topic"`         // topic名称
	TopicType     TopicType      `json:"topicType"`     // topic类型
	IsSystemTopic bool           `json:"isSystemTopic"` // 是否为系统Topic
}

type TopicVos []*TopicVo

func (self TopicVos) Less(i, j int) bool {
	iq := self[i]
	jq := self[j]

	if iq.Topic < jq.Topic {
		return true
	} else if iq.Topic > jq.Topic {
		return false
	}
	return false
}

func (self TopicVos) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self TopicVos) Len() int {
	return len(self)
}

// TopicConfigVo topic配置项
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type TopicConfigVo struct {
	BrokerAddr     string `json:"brokerAddr"`     // broker地址
	BrokerId       int    `json:"brokerId"`       // brokerid
	BrokerName     string `json:"brokerName"`     // broker名称
	WriteQueueNums int    `json:"writeQueueNums"` // 写队列数
	Unit           bool   `json:"unit"`           // 是否为单元topic
	ReadQueueNums  int    `json:"readQueueNums"`  // 读队列数
	Order          bool   `json:"order"`          // 是否为顺序topic
	Perm           int    `json:"perm"`           // 对应的broker读写权限
}

// ToString 打印TopicVo结构体内容
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (t *TopicVo) ToString() string {
	if t == nil {
		return fmt.Sprintf("TopicVo is nil")
	}

	format := "TopicVo {topic=%s, clusterName=%s, brokerName=%s, brokerAddr=%s, brokerId=%d, readQueueNums=%d, writeQueueNums=%d, isSystemTopic=%t}"
	return fmt.Sprintf(format, t.Topic, t.ClusterName, t.TopicConfigVo.BrokerName, t.TopicConfigVo.BrokerAddr, t.TopicConfigVo.BrokerId,
		t.TopicConfigVo.ReadQueueNums, t.TopicConfigVo.WriteQueueNums, IsSystemTopic(t.Topic))
}

// ToTopicVo 转化为TopicVo
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func ToTopicVo(wapper *body.TopicBrokerClusterWapper) *TopicVo {
	topicVo := &TopicVo{}
	topicVo.TopicType = ParseTopicType(wapper.TopicName)
	topicVo.Topic = wapper.TopicName
	topicVo.ClusterName = wapper.ClusterName
	topicVo.IsSystemTopic = IsSystemTopic(wapper.TopicName)
	topicVo.TopicConfigVo = new(TopicConfigVo)

	topicVo.TopicConfigVo.Perm = wapper.TopicUpdateConfigWapper.Perm
	topicVo.TopicConfigVo.BrokerAddr = wapper.TopicUpdateConfigWapper.BrokerAddr
	topicVo.TopicConfigVo.WriteQueueNums = wapper.TopicUpdateConfigWapper.WriteQueueNums
	topicVo.TopicConfigVo.ReadQueueNums = wapper.TopicUpdateConfigWapper.ReadQueueNums
	topicVo.TopicConfigVo.Unit = wapper.TopicUpdateConfigWapper.Unit
	topicVo.TopicConfigVo.Order = wapper.TopicUpdateConfigWapper.Order
	topicVo.TopicConfigVo.BrokerName = wapper.TopicUpdateConfigWapper.BrokerName
	topicVo.TopicConfigVo.BrokerId = wapper.TopicUpdateConfigWapper.BrokerId
	return topicVo
}

// ToTopicConfig 转化为stgcommon.TopicConfig
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (t *CreateTopic) ToTopicConfig() *stgcommon.TopicConfig {
	perm := constant.PERM_READ | constant.PERM_WRITE
	queueNum := int32(8)
	topicConfig := stgcommon.NewDefaultTopicConfig(t.Topic, queueNum, queueNum, perm, stgcommon.SINGLE_TAG)
	return topicConfig
}

// ToTopicConfig 转化为stgcommon.TopicConfig
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (t *UpdateTopic) ToTopicConfig() *stgcommon.TopicConfig {
	perm := constant.PERM_READ | constant.PERM_WRITE
	topicConfig := stgcommon.NewDefaultTopicConfig(t.Topic, int32(t.ReadQueueNums), int32(t.WriteQueueNums), perm, stgcommon.SINGLE_TAG)
	return topicConfig
}

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

// IsSystemTopic 是否为系统Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func IsSystemTopic(topic string) bool {
	if strings.Contains(topic, "broker-") {
		return true //TODO: 约定以“broker-”开始的都是系统topic，后续待优化
	}

	for _, value := range SystemTopics {
		if value == topic {
			return true
		}
	}
	return false
}

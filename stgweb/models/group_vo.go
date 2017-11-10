package models

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)

// ConsumerGroup 消费组
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ConsumerGroup struct {
	BrokerOffset   int64 `json:"brokerOffset"`
	ConsumerOffset int64 `json:"consumerOffset"`
	Diff           int64 `json:"diff"`
	*message.MessageQueue
}

// ConsumerProgress 消费进度
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ConsumerProgress struct {
	GroupId   string           `json:"consumeGroupId"`
	Tps       int64            `json:"consumeTps"`
	DiffTotal int64            `json:"diffTotal"`
	List      []*ConsumerGroup `json:"data"`
	Total     int64            `json:"total"`
}

// ConsumerGroupList 消费组列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ConsumerGroupVo struct {
	ClusterName     string    `json:"clusterName"`     // 集群名称
	Topic           string    `json:"topic"`           // 集群名称
	TopicType       TopicType `json:"topicType"`       // topic类型
	ConsumerGroupId string    `json:"consumerGroupId"` // 消费组ID
}

// NewConsumerGroupVo 初始化ConsumerGroupVo
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/10
func NewConsumerGroupVo(clusterName, topic, consumerGroupId string) *ConsumerGroupVo {
	consumerGroupVo := &ConsumerGroupVo{
		ClusterName:     clusterName,
		Topic:           topic,
		TopicType:       ParseTopicType(topic),
		ConsumerGroupId: consumerGroupId,
	}
	return consumerGroupVo
}

// BrokerRuntimeInfo broker运行状态
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type BrokerRuntimeInfo struct {
	BrokerVersionDesc           string  `json:"brokerVersionDesc"`
	BrokerVersion               string  `json:"brokerVersion"`
	MsgPutTotalYesterdayMorning string  `json:"msgPutTotalYesterdayMorning"`
	MsgPutTotalTodayMorning     string  `json:"msgPutTotalTodayMorning"`
	MsgPutTotalTodayNow         string  `json:"msgPutTotalTodayNow"`
	MsgGetTotalYesterdayMorning string  `json:"msgGetTotalYesterdayMorning"`
	MsgGetTotalTodayNow         string  `json:"msgGetTotalTodayNow"`
	SendThreadPoolQueueSize     string  `json:"sendThreadPoolQueueSize"`
	SendThreadPoolQueueCapacity string  `json:"sendThreadPoolQueueCapacity"`
	MsgGetTotalTodayMorning     string  `json:"msgGetTotalTodayMorning"`
	InTps                       float64 `json:"inTps"`
	OutTps                      float64 `json:"outTps"`
}

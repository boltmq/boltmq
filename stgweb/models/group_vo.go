package models

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/admin"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"strconv"
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
	ConsumeGroupId string           `json:"consumeGroupId"`
	Tps            float64          `json:"consumeTps"` // 保留2位小数
	DiffTotal      int64            `json:"diffTotal"`
	Data           []*ConsumerGroup `json:"data"`
	Total          int64            `json:"total"`
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

// NewConsumerGroup 初始化ConsumerGroup
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/10
func NewConsumerGroup() *ConsumerGroup {
	consumerGroup := &ConsumerGroup{}
	consumerGroup.MessageQueue = new(message.MessageQueue)
	return consumerGroup
}

// ToConsumerGroup 转化为ConsumerGroup
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/10
func ToConsumerGroup(mq *message.MessageQueue, wapper *admin.OffsetWrapper) (consumerGroup *ConsumerGroup, diff int64) {
	consumerGroup = NewConsumerGroup()
	consumerGroup.Topic = mq.Topic
	consumerGroup.BrokerName = mq.BrokerName
	consumerGroup.QueueId = mq.QueueId

	if wapper != nil {
		consumerGroup.ConsumerOffset = wapper.ConsumerOffset
		consumerGroup.BrokerOffset = wapper.BrokerOffset
		diff = wapper.BrokerOffset - wapper.ConsumerOffset
		consumerGroup.Diff = diff
	}
	return consumerGroup, diff
}

// NewConsumerProgress 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/10
func NewConsumerProgress(data []*ConsumerGroup, total, diffTotal int64, consumeGroupId string, tps float64) *ConsumerProgress {
	consumerProgress := &ConsumerProgress{
		Data:           data,
		Total:          total,
		ConsumeGroupId: consumeGroupId,
		DiffTotal:      diffTotal,
		Tps:            tps,
	}
	return consumerProgress
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

// ToCluterGeneral 转化CluterGeneral
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func (self *BrokerRuntimeInfo) ToCluterGeneral(brokerAddr, brokerName string, brokerId int) *ClusterGeneral {
	clusterGeneral := &ClusterGeneral{}
	clusterGeneral.BrokerAddr = brokerAddr
	clusterGeneral.BrokerId = brokerId
	clusterGeneral.BrokerName = brokerName
	clusterGeneral.Version = self.BrokerVersion
	clusterGeneral.VersionDesc = self.BrokerVersionDesc
	clusterGeneral.InTPS = JSONFloat(self.InTps)
	clusterGeneral.OutTPS = JSONFloat(self.OutTps)

	brokerRole := "slave"
	if brokerId == stgcommon.MASTER_ID {
		brokerRole = "master"
	}
	clusterGeneral.BrokerRole = brokerRole

	msgPutTotalYesterdayMorning, _ := strconv.ParseInt(self.MsgPutTotalYesterdayMorning, 10, 64)
	msgPutTotalTodayMorning, _ := strconv.ParseInt(self.MsgPutTotalTodayMorning, 10, 64)
	msgPutTotalTodayNow, _ := strconv.ParseInt(self.MsgPutTotalTodayNow, 10, 64)

	msgGetTotalYesterdayMorning, _ := strconv.ParseInt(self.MsgGetTotalYesterdayMorning, 10, 64)
	msgGetTotalTodayMorning, _ := strconv.ParseInt(self.MsgGetTotalTodayMorning, 10, 64)
	msgGetTotalTodayNow, _ := strconv.ParseInt(self.MsgGetTotalTodayNow, 10, 64)

	inTotalYest := msgPutTotalTodayMorning - msgPutTotalYesterdayMorning
	outTotalYest := msgGetTotalTodayMorning - msgGetTotalYesterdayMorning

	inTotalToday := msgPutTotalTodayNow - msgPutTotalTodayMorning
	outTotalToday := msgGetTotalTodayNow - msgGetTotalTodayMorning

	clusterGeneral.InTotalYest = inTotalYest
	clusterGeneral.OutTotalYest = outTotalYest
	clusterGeneral.InTotalToday = inTotalToday
	clusterGeneral.OutTotalToday = outTotalToday

	return clusterGeneral
}

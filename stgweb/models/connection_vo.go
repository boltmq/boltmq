package models

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/mqversion"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
)

// ConnectionOnline 在线进程列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ConnectionOnline struct {
	ClusterName      string   `json:"clusterName"`      // 集群名称
	Topic            string   `json:"topic"`            // 集群名称
	ProduceNums      int      `json:"produceNums"`      // 生产进程总数
	ConsumerGroupIds []string `json:"consumerGroupIds"` // 消费组ID
	ConsumeNums      int      `json:"consumeNums"`      // 消费进程总数
}

type ConnectionOnlines []*ConnectionOnline

// ConnectionDetail 在线进程详情
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ConnectionDetail struct {
	ConsumerOnLine *ConsumerOnLine `json:"consumer"`
	ProducerOnLine *ProducerOnLine `json:"producer"`
}

// ProducerOnLine 在线生产进程
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ProducerOnLine struct {
	ClusterName     string             `json:"clusterName"`     // 集群名称
	Topic           string             `json:"topic"`           // topic名称
	ProducerGroupId string             `json:"producerGroupId"` // 生产组者组ID
	Describe        string             `json:"describe"`        // 查询结果的描述
	Connection      []*body.Connection `json:"groups"`          // 在线生产进程
}

// ConsumerOnLine 在线消费进程
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ConsumerOnLine struct {
	ClusterName string                  `json:"clusterName"` // 集群名称
	Topic       string                  `json:"topic"`       // topic名称
	Describe    string                  `json:"describe"`    // 查询结果的描述
	Connection  []*ConsumerConnectionVo `json:"groups"`      // 在线消费进程
}

// ConsumerConnection 消费者进程
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/7/14
type ConsumerConnectionVo struct {
	ConsumerGroupId     string                 `json:"consumerGroupId"`     // 消费者组ID
	ClientId            string                 `json:"clientId"`            // 消费者客户端实例
	ClientAddr          string                 `json:"clientAddr"`          // 消费者客户端地址
	Language            string                 `json:"language"`            // 客户端语言
	VersionDesc         string                 `json:"versionDesc"`         // mq版本号描述
	Version             int                    `json:"version"`             // mq版本号
	ConsumeTps          float64                `json:"consumeTps"`          // 实时消费Tps
	ConsumeFromWhere    string                 `json:"consumeFromWhere"`    // 从哪里开始消费
	ConsumeType         string                 `json:"consumeType"`         // 消费类型(主动、被动)
	DiffTotal           int64                  `json:"diffTotal"`           // 消息堆积总数
	MessageModel        string                 `json:"messageModel"`        // 消息模式(集群、广播)
	SubscribeTopicTable []*SubscribeTopicTable `json:"subscribeTopicTable"` // 消费者订阅Topic列表
}

type SubscribeTopicTable struct {
	Topic           string   `json:"topic"`
	SubString       string   `json:"tags"`
	ClassFilterMode bool     `json:"classFilterMode"`
	TagsSet         []string `json:"tagsSet"`
	CodeSet         []int32  `json:"codeSet"`
	SubVersion      int64    `json:"subVersion"`
}

func (self ConnectionOnlines) Less(i, j int) bool {
	iq := self[i]
	jq := self[j]

	if iq.Topic < jq.Topic {
		return true
	} else if iq.Topic > jq.Topic {
		return false
	}
	return false
}

func (self ConnectionOnlines) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self ConnectionOnlines) Len() int {
	return len(self)
}

// NewConnectionOnline 初始化ConnectionOnline
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/10
func NewConnectionOnline(clusterName, topic string, consumerGroupIds []string, consumeNums, produceNums int) *ConnectionOnline {
	connectionOnline := &ConnectionOnline{
		ClusterName:      clusterName,
		Topic:            topic,
		ConsumerGroupIds: consumerGroupIds,
		ConsumeNums:      consumeNums,
		ProduceNums:      produceNums,
	}
	return connectionOnline
}

// ToSubscribeTopicTables 消费者订阅Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/7/14
func ToSubscribeTopicTables(cc *body.ConsumerConnectionPlus) (subscribeTables []*SubscribeTopicTable) {
	subscribeTables = make([]*SubscribeTopicTable, 0)
	for topic, data := range cc.SubscriptionTable {
		if data == nil {
			continue
		}

		subscribeTable := &SubscribeTopicTable{
			Topic:           topic,
			SubString:       data.SubString,
			ClassFilterMode: data.ClassFilterMode,
			SubVersion:      int64(data.SubVersion),
			TagsSet:         data.TagsSet,
			CodeSet:         data.CodeSet,
		}
		//fmt.Println("subscribeTable  >>>>>>>>>> ", subscribeTable)
		subscribeTables = append(subscribeTables, subscribeTable)
	}
	return subscribeTables
}

// ToConsumerConnectionVo 转化为消费进程对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/7/14
func ToConsumerConnectionVo(c *body.Connection, cc *body.ConsumerConnectionPlus, progress *ConsumerProgress, consumerGroupId string) *ConsumerConnectionVo {
	consumerConnectionVo := &ConsumerConnectionVo{
		ConsumerGroupId:     consumerGroupId,
		ClientId:            c.ClientId,
		ClientAddr:          c.ClientAddr,
		Language:            c.Language,
		Version:             int(c.Version),
		VersionDesc:         mqversion.GetVersionDesc(int(c.Version)),
		ConsumeTps:          progress.Tps,
		DiffTotal:           progress.DiffTotal,
		ConsumeFromWhere:    cc.ConsumeFromWhere.ToString(),
		ConsumeType:         cc.ConsumeType.ToString(),
		MessageModel:        cc.MessageModel.ToString(),
		SubscribeTopicTable: ToSubscribeTopicTables(cc), // 订阅Topic列表
	}

	return consumerConnectionVo
}

// String 打印SubscribeTopicTable数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/16
func (s *SubscribeTopicTable) String() string {
	format := "SubscribeTopicTable {topic=%s, tags=%s, tagsSet=[%v], codeSet=[%v], classFilterMode=%t, subVersion=%d}"
	return fmt.Sprintf(format, s.Topic, s.SubString, s.TagsSet, s.CodeSet, s.ClassFilterMode, s.SubVersion)
}

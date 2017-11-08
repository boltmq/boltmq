package models

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message/track"
)

// MessageBodyVo 单独查询Body消息内容
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type MessageBodyVo struct {
	MsgBody string `json:"msgBody"` // 消息内容
	MsgId   string `json:"msgId"`   // 消息ID
}

// ConsumeConnectionVo 消费进程列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ConsumeConnectionVo struct {
	IP  string `json:"ip"`  // 消费进程所在的服务器IP
	PID int64  `json:"pid"` // 进程pid
}

// ConsumeExtVo 消费轨迹-消费节点
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ConsumeTrackVo struct {
	Connection      []*ConsumeConnectionVo `json:"connection"`
	ConsumerGroupId string                 `json:"consumerGroupId"` // 消费组ID
	Code            int                    `json:"code"`            // 消息消费结果 0:消费成功, 1:失败
}

// ProduceTrackVo 消费轨迹-生产节点
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ProduceTrackVo struct {
	BornIp            string `json:"bornIp"`            // 消息来源IP
	BornTimestamp     string `json:"bornTimestamp"`     // 消息在客户端创建时间
	Code              int    `json:"code"`              // 消息发送结果 0:消费成功, 1:失败
	ProducerGroupId   string `json:"producerGroupId"`   // 生产组ID
	SendTimeConsuming int    `json:"sendTimeConsuming"` // 发送消息耗时(毫秒)
}

// TopicTrackVo 消费轨迹-topic节点
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type TopicTrackVo struct {
	Key   string `json:"key"`   // 消息key
	Tag   string `json:"tag"`   // 消息Tag
	Topic string `json:"topic"` // topic名称
}

// MessageTrackVo 消息轨迹节点
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type MessageTrackExt struct {
	ConsumeExt []*ConsumeTrackVo `json:"consumeExt"` // 消费节点
	ProduceExt *ProduceTrackVo   `json:"produceExt"` // 生产节点
	TopicExt   *TopicTrackVo     `json:"topicExt"`   // topic节点
}

// MessageTrackVo 消息轨迹节点
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type MessageTrackBase struct {
	MessageTrack []*MessageTrackExt `json:"track"`        // 消息消费轨迹
	CurrentMsgId string             `json:"currentMsgId"` // 当前消息ID
	OriginMsgId  string             `json:"originMsgId"`  // 源消息ID
}

// BlotMessage 消息消费结果查询、消费轨迹
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type BlotMessage struct {
	Base     *message.Message      `json:"base"`     // 消息基础属性
	Track    []*track.MessageTrack `json:"track"`    // 消息消费结果
	BodyPath string                `json:"bodyPath"` // 消息body内容存储在web服务器的路径
}

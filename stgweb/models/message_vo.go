package models

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
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

// ConnectionVo 消费进程列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ConnectionVo struct {
	IP  string `json:"ip"`  // 消费进程所在的服务器IP
	PID int64  `json:"pid"` // 进程pid
}

// ConsumeExtVo 消费轨迹-消费节点
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ConsumeTrackVo struct {
	Connection      []*ConnectionVo `json:"connection"`
	ConsumerGroupId string          `json:"consumerGroupId"` // 消费组ID
	Code            int             `json:"code"`            // 消息消费结果 0:消费成功, 1:失败
	TrackType       int             `json:"trackType"`       // 消息轨迹类型
}

// ProduceTrackVo 消费轨迹-生产节点
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ProduceTrackVo struct {
	BornIp            string `json:"bornIp"`            // 消息来源IP
	BornTimestamp     string `json:"bornTimestamp"`     // 消息在客户端创建时间
	Code              int    `json:"code"`              // 消息发送结果 0:消费成功, 1:失败
	ProducerGroupId   string `json:"producerGroupId"`   // 生产组ID
	SendTimeConsuming int64  `json:"sendTimeConsuming"` // 发送消息耗时(毫秒)
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
// Since: 2017/11/13
type MessageTrackExtWapper struct {
	TrackWapper  *MessageTrackExt `json:"track"`
	CurrentMsgId string           `json:"currentMsgId"` // 当前消息ID
	OriginMsgId  string           `json:"originMsgId"`  // 源消息ID
}

// MessageTrackVo 消息轨迹节点
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type MessageTrackBase struct {
	MessageTrack []*MessageTrackExt `json:"track"`        // 消息消费轨迹
	CurrentMsgId string             `json:"currentMsgId"` // 当前消息ID
	OriginMsgId  string             `json:"originMsgId"`  // 源消息ID
}

type MessageExtVo struct {
	Topic                     string            `json:"topic"`                     // 消息主题
	Flag                      int32             `json:"flag"`                      // 消息标志，系统不做干预，完全由应用决定如何使用
	Properties                map[string]string `json:"properties"`                // 消息属性，都是系统属性，禁止应用设置
	Body                      string            `json:"body"`                      // 消息体
	QueueId                   int32             `json:"queueId"`                   // 队列ID<PUT>
	StoreSize                 int32             `json:"storeSize"`                 // 存储记录大小
	QueueOffset               int64             `json:"queueOffset"`               // 队列偏移量
	SysFlag                   int32             `json:"sysFlag"`                   // 消息标志位 <PUT>
	BornTimestamp             string            `json:"bornTimestamp"`             // 消息在客户端创建时间戳 <PUT>
	BornHost                  string            `json:"bornHost"`                  // 消息来自哪里 <PUT>
	StoreTimestamp            string            `json:"storeTimestamp"`            // 消息在服务器存储时间戳
	StoreHost                 string            `json:"storeHost"`                 // 消息存储在哪个服务器 <PUT>
	MsgId                     string            `json:"msgId"`                     // 消息ID
	CommitLogOffset           int64             `json:"commitLogOffset"`           // 消息对应的Commit Log Offset
	BodyCRC                   int32             `json:"bodyCRC"`                   // 消息体CRC
	ReconsumeTimes            int32             `json:"reconsumeTimes"`            // 当前消息被某个订阅组重新消费了几次（订阅组之间独立计数）
	PreparedTransactionOffset int64             `json:"preparedTransactionOffset"` // 事务预处理偏移量
}

// BlotMessage 消息消费结果查询、消费轨迹
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type BlotMessage struct {
	Base     *MessageExtVo         `json:"base"`     // 消息基础属性
	Track    []*track.MessageTrack `json:"track"`    // 消息消费结果
	BodyPath string                `json:"bodyPath"` // 消息body内容存储在web服务器的路径
}

// NewMessageTrackExtWapper 初始化MessageTrackExtWapper
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func NewMessageTrackExtWapper(currentMsgId string) *MessageTrackExtWapper {
	messageTrackExtWapper := &MessageTrackExtWapper{
		TrackWapper:  new(MessageTrackExt),
		CurrentMsgId: currentMsgId,
	}
	return messageTrackExtWapper
}

// NewMessageBodyVo 初始化MessageBodyVo
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func NewMessageBodyVo(msgId, msgBody string) *MessageBodyVo {
	messageBodyVo := &MessageBodyVo{
		MsgId:   msgId,
		MsgBody: msgBody,
	}
	return messageBodyVo
}

// NewBlotMessage 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func NewBlotMessage(base *MessageExtVo, messageTrack []*track.MessageTrack, msgBodyPath string) *BlotMessage {
	blotMessage := &BlotMessage{
		Base:     base,
		Track:    messageTrack,
		BodyPath: msgBodyPath,
	}
	return blotMessage
}

// ToMessageExtVo 转化为MessageExtVo
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func ToMessageExtVo(msg *message.MessageExt) *MessageExtVo {
	messageExtVo := &MessageExtVo{
		Topic:                     msg.Topic,
		Flag:                      msg.Flag,
		Properties:                msg.Properties,
		Body:                      string(msg.Body),
		QueueId:                   msg.QueueId,
		StoreSize:                 msg.StoreSize,
		QueueOffset:               msg.QueueOffset,
		SysFlag:                   msg.SysFlag,
		BornTimestamp:             stgcommon.FormatTimestamp(msg.BornTimestamp),
		BornHost:                  msg.BornHost,
		StoreTimestamp:            stgcommon.FormatTimestamp(msg.StoreTimestamp),
		StoreHost:                 msg.StoreHost,
		MsgId:                     msg.MsgId,
		CommitLogOffset:           msg.CommitLogOffset,
		BodyCRC:                   msg.BodyCRC,
		ReconsumeTimes:            msg.ReconsumeTimes,
		PreparedTransactionOffset: msg.PreparedTransactionOffset,
	}
	return messageExtVo
}

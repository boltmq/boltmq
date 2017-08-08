package producer
// 发送消息返回结果结构体
// Author: yintongqiang
// Since:  2017/8/8

import "git.oschina.net/cloudzone/smartgo/stgcommon/message"

type SendResult struct {
	SendStatus    string
	MsgId         string
	MessageQueue  message.MessageQueue
	QueueOffset   int64
	TransactionId string
}
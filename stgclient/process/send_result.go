package process
// 发送消息返回结果结构体
// Author: yintongqiang
// Since:  2017/8/8

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"strings"
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"strconv"
)

type SendResult struct {
	SendStatus    SendStatus
	MsgId         string
	MessageQueue  message.MessageQueue
	QueueOffset   int64
	TransactionId string
}

func NewSendResult(sendStatus SendStatus, msgId string, messageQueue message.MessageQueue, queueOffset int64, projectGroupPrefix string) *SendResult {
	if !strings.EqualFold(projectGroupPrefix, "") {
		messageQueue.Topic = stgclient.ClearProjectGroup(messageQueue.Topic, projectGroupPrefix)
	}
	return &SendResult{
		SendStatus:sendStatus,
		MsgId:msgId,
		MessageQueue:messageQueue,
		QueueOffset:queueOffset}
}
func (sendResult *SendResult)ToString() string {
	return "SendResult [sendStatus=" + sendResult.SendStatus.String() + ", msgId=" + sendResult.MsgId + ", messageQueue=" + sendResult.MessageQueue.ToString() + ", queueOffset=" + strconv.Itoa(int(sendResult.QueueOffset)) + "]"
}
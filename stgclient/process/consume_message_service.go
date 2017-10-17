package process

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
)

// ConsumeMessageService: 消费消息服务接口
// Author: yintongqiang
// Since:  2017/8/11
type ConsumeMessageService interface {
	Start()    // 开启
	Shutdown() // 关闭
	ConsumeMessageDirectly(msg *message.MessageExt, brokerName string) *body.ConsumeMessageDirectlyResult
	SubmitConsumeRequest(msgs []*message.MessageExt, processQueue *consumer.ProcessQueue, messageQueue *message.MessageQueue, dispathToConsume bool) // 提交消费请求
}

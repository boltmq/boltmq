package process

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)

// ConsumeMessageService: 消费消息服务接口
// Author: yintongqiang
// Since:  2017/8/11

type ConsumeMessageService interface {
	// 开启
	Start()
	// 关闭
	Shutdown()
	// 提交消费请求
	SubmitConsumeRequest(msgs []*message.MessageExt, processQueue *consumer.ProcessQueue, messageQueue *message.MessageQueue, dispathToConsume bool)
}

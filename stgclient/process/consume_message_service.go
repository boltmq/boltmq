package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
)
// ConsumeMessageService: 消费消息服务接口
// Author: yintongqiang
// Since:  2017/8/11

type ConsumeMessageService interface {
	Start()
	Shutdown()
	SubmitConsumeRequest(msgs []message.MessageExt, processQueue *consumer.ProcessQueue, messageQueue *message.MessageQueue, dispathToConsume bool)
}

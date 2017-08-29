package consumer

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/listener"
)
// MessageListenerConcurrently: 普通消息消费接口
// Author: yintongqiang
// Since:  2017/8/10

type MessageListenerConcurrently interface {
	ConsumeMessage(msgs []*message.MessageExt, context ConsumeConcurrentlyContext) listener.ConsumeConcurrentlyStatus
}

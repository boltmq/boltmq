package consumer

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"math"
)

// ConsumeConcurrentlyContext: 普通消息消费上下文
// Author: yintongqiang
// Since:  2017/8/10

type ConsumeConcurrentlyContext struct {
	MessageQueue              *message.MessageQueue
	// 消费失败延迟消费级别
	DelayLevelWhenNextConsume int
	AckIndex                  int
}

func NewConsumeConcurrentlyContext(mq *message.MessageQueue) *ConsumeConcurrentlyContext {
	return &ConsumeConcurrentlyContext{mq,0,math.MaxInt32}
}

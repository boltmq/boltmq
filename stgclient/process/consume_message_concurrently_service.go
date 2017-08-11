package process

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
)
// ConsumeMessageConcurrentlyService: 普通消费服务
// Author: yintongqiang
// Since:  2017/8/11

type ConsumeMessageConcurrentlyService struct {
	defaultMQPushConsumerImpl *DefaultMQPushConsumerImpl
	defaultMQPushConsumer     *DefaultMQPushConsumer
	messageListener           consumer.MessageListenerConcurrently
	consumerGroup             string
	// 模拟线程池
	consumeExecutor           chan int
}

func NewConsumeMessageConcurrentlyService(defaultMQPushConsumerImpl *DefaultMQPushConsumerImpl, messageListener consumer.MessageListenerConcurrently) *ConsumeMessageConcurrentlyService {
	return &ConsumeMessageConcurrentlyService{defaultMQPushConsumerImpl:defaultMQPushConsumerImpl,
		defaultMQPushConsumer:defaultMQPushConsumerImpl.defaultMQPushConsumer,
		consumerGroup:defaultMQPushConsumerImpl.defaultMQPushConsumer.consumerGroup,
		consumeExecutor:make(chan int, defaultMQPushConsumerImpl.defaultMQPushConsumer.consumeThreadMax),
		messageListener:messageListener}
}

func ( service *ConsumeMessageConcurrentlyService)Start() {

}

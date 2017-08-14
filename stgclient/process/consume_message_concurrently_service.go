package process

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
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

type consumeRequest struct {
	msgs         []message.MessageExt
	processQueue consumer.ProcessQueue
	messageQueue message.MessageQueue
	ConsumeMessageConcurrentlyService
}

func (consume *consumeRequest)run() {
	if consume.processQueue.Dropped {
		logger.Info("the message queue not be able to consume, because it's dropped")
		return
	}
	var listener consumer.MessageListenerConcurrently = consume.messageListener.(consumer.MessageListenerConcurrently)
	context := consumer.ConsumeConcurrentlyContext{MessageQueue:consume.messageQueue}
	listener.ConsumeMessage(consume.msgs, context)
}

func NewConsumeMessageConcurrentlyService(defaultMQPushConsumerImpl *DefaultMQPushConsumerImpl, messageListener consumer.MessageListenerConcurrently) *ConsumeMessageConcurrentlyService {
	return &ConsumeMessageConcurrentlyService{defaultMQPushConsumerImpl:defaultMQPushConsumerImpl,
		defaultMQPushConsumer:defaultMQPushConsumerImpl.defaultMQPushConsumer,
		consumerGroup:defaultMQPushConsumerImpl.defaultMQPushConsumer.consumerGroup,
		consumeExecutor:make(chan int, defaultMQPushConsumerImpl.defaultMQPushConsumer.consumeThreadMax),
		messageListener:messageListener}
}

func (service *ConsumeMessageConcurrentlyService)Start() {

}

func (service *ConsumeMessageConcurrentlyService)SubmitConsumeRequest(msgs []message.MessageExt, processQueue consumer.ProcessQueue, messageQueue message.MessageQueue, dispathToConsume bool) {
	consumeBatchSize := service.defaultMQPushConsumer.consumeMessageBatchMaxSize
	if len(msgs) <= consumeBatchSize {

	}
}


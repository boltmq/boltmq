package process

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/listener"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	set "github.com/deckarep/golang-set"
	"time"
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
	processQueue *consumer.ProcessQueue
	messageQueue *message.MessageQueue
	*ConsumeMessageConcurrentlyService
}

func (consume *consumeRequest)run() {
	defer func() {
		<-consume.consumeExecutor
	}()
	if consume.processQueue.Dropped {
		logger.Infof("the message queue not be able to consume, because it's dropped")
		return
	}
	var listener consumer.MessageListenerConcurrently = consume.messageListener.(consumer.MessageListenerConcurrently)
	context := consumer.ConsumeConcurrentlyContext{MessageQueue:consume.messageQueue}
	status := listener.ConsumeMessage(consume.msgs, context)
	//todo 消费统计
	if !consume.processQueue.Dropped {
		consume.processConsumeResult(status, context, consume)
	}
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

func (service *ConsumeMessageConcurrentlyService)Shutdown() {
	// 关闭通道
	close(service.consumeExecutor)
}

func (service *ConsumeMessageConcurrentlyService)sendMessageBack(msg message.MessageExt, context consumer.ConsumeConcurrentlyContext) bool {
	service.defaultMQPushConsumerImpl.sendMessageBack(msg, context.DelayLevelWhenNextConsume, context.MessageQueue.BrokerName)
	return true
}
func (service *ConsumeMessageConcurrentlyService)processConsumeResult(status listener.ConsumeConcurrentlyStatus,
context consumer.ConsumeConcurrentlyContext, consumeRequest *consumeRequest) {
	ackIndex := context.AckIndex
	if len(consumeRequest.msgs) == 0 {
		return
	}
	switch status {
	case listener.CONSUME_SUCCESS:
		if ackIndex >= len(consumeRequest.msgs) {
			ackIndex = len(consumeRequest.msgs) - 1
		}
	//ok:=ackIndex+1
	//len(consumeRequest.msgs)-ok
	//todo 消费统计
	case listener.RECONSUME_LATER:
		ackIndex = -1
	//todo 消费统计
	default:

	}
	switch service.defaultMQPushConsumer.messageModel {
	//todo 广播后续添加
	case heartbeat.BROADCASTING:
	case heartbeat.CLUSTERING:
		msgBackFailed := []message.MessageExt{}
		msgList := []message.MessageExt{}
		msgSet := set.NewSet()
		for _, msg := range consumeRequest.msgs {
			msgSet.Add(msg)
		}
		for i := ackIndex + 1; i < len(consumeRequest.msgs); i++ {
			msg := consumeRequest.msgs[i]
			if !service.sendMessageBack(msg, context) {
				msg.ReconsumeTimes = msg.ReconsumeTimes + 1
				msgBackFailed = append(msgBackFailed, msg)
				msgSet.Remove(msg)
			}
		}

		if len(msgBackFailed) > 0 {
			for val := range msgSet.Iterator().C {
				msgList = append(msgList, val.(message.MessageExt))
			}
			consumeRequest.msgs = msgList
			service.submitConsumeRequestLater(msgBackFailed, consumeRequest.processQueue, consumeRequest.messageQueue)
		}
	}
	offset := consumeRequest.processQueue.RemoveMessage(consumeRequest.msgs)
	if offset >= 0 {
		service.defaultMQPushConsumerImpl.OffsetStore.UpdateOffset(consumeRequest.messageQueue, offset, true)
	}

}

func (service *ConsumeMessageConcurrentlyService)submitConsumeRequestLater(msgs []message.MessageExt, processQueue *consumer.ProcessQueue, messageQueue *message.MessageQueue) {
	go func() {
		time.Sleep(time.Second * 5)
		service.SubmitConsumeRequest(msgs, processQueue, messageQueue, true)
	}()
}

func (service *ConsumeMessageConcurrentlyService)SubmitConsumeRequest(msgs []message.MessageExt, processQueue *consumer.ProcessQueue, messageQueue *message.MessageQueue, dispathToConsume bool) {
	consumeBatchSize := service.defaultMQPushConsumer.consumeMessageBatchMaxSize
	if len(msgs) <= consumeBatchSize {
		consumeRequest := &consumeRequest{msgs:msgs, processQueue:processQueue, messageQueue:messageQueue, ConsumeMessageConcurrentlyService:service}
		service.consumeExecutor <- 1
		go consumeRequest.run()
	} else {
		for total := 0; total < len(msgs); {
			msgThis := []message.MessageExt{}
			for i := 0; i < consumeBatchSize; i++ {
				total++
				if total < len(msgs) {
					msgThis = append(msgs, msgs[total])
				} else {
					break
				}
			}
			consumeRequest := &consumeRequest{msgs:msgThis, processQueue:processQueue, messageQueue:messageQueue, ConsumeMessageConcurrentlyService:service}
			service.consumeExecutor <- 1
			go consumeRequest.run()
		}
	}
}


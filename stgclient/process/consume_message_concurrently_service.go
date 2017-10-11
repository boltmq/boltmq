package process

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/listener"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	set "github.com/deckarep/golang-set"
	"strings"
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
	consumeExecutor chan int
}

type consumeRequest struct {
	msgs         []*message.MessageExt
	processQueue *consumer.ProcessQueue
	messageQueue *message.MessageQueue
	*ConsumeMessageConcurrentlyService
}

func (consume *consumeRequest) run() {
	defer func() {
		<-consume.consumeExecutor
	}()
	if consume.processQueue.Dropped {
		logger.Infof("the message queue not be able to consume, because it's dropped")
		return
	}
	var msgListener consumer.MessageListenerConcurrently = consume.messageListener.(consumer.MessageListenerConcurrently)
	context := consumer.NeWConsumeConcurrentlyContext(consume.messageQueue)
	groupTopic := stgcommon.GetRetryTopic(consume.consumerGroup)
	for _, msg := range consume.msgs {
		retryTopic := msg.GetProperty(message.PROPERTY_RETRY_TOPIC)
		if !strings.EqualFold(retryTopic, "") && strings.EqualFold(groupTopic, msg.Topic) {
			msg.Topic = retryTopic
		}
	}
	status := msgListener.ConsumeMessage(consume.msgs, context)
	// 用于客户端返回不正常处理
	if status != listener.CONSUME_SUCCESS && status != listener.RECONSUME_LATER {
		logger.Warnf("consumeMessage return error, Group: %v Msgs: %v MQ: %v",consume.consumerGroup,consume.msgs,consume.messageQueue.ToString())
		status = listener.RECONSUME_LATER
	}
	//todo 消费统计
    // 处理队列没有drop对消费结果进行处理
	if !consume.processQueue.Dropped {
		consume.processConsumeResult(status, context, consume)
	}
}

func NewConsumeMessageConcurrentlyService(defaultMQPushConsumerImpl *DefaultMQPushConsumerImpl, messageListener consumer.MessageListenerConcurrently) *ConsumeMessageConcurrentlyService {
	return &ConsumeMessageConcurrentlyService{defaultMQPushConsumerImpl: defaultMQPushConsumerImpl,
		defaultMQPushConsumer: defaultMQPushConsumerImpl.defaultMQPushConsumer,
		consumerGroup:         defaultMQPushConsumerImpl.defaultMQPushConsumer.consumerGroup,
		consumeExecutor:       make(chan int, defaultMQPushConsumerImpl.defaultMQPushConsumer.consumeThreadMax),
		messageListener:       messageListener}
}

// 仅仅为了实现接口
func (service *ConsumeMessageConcurrentlyService) Start() {

}

func (service *ConsumeMessageConcurrentlyService) Shutdown() {
	// 关闭通道
	close(service.consumeExecutor)
}

// 消费不了重发到重试队列
func (service *ConsumeMessageConcurrentlyService) sendMessageBack(msg *message.MessageExt, context *consumer.ConsumeConcurrentlyContext) bool {
	service.defaultMQPushConsumerImpl.sendMessageBack(msg, context.DelayLevelWhenNextConsume, context.MessageQueue.BrokerName)
	return true
}

// 处理消费结果
func (service *ConsumeMessageConcurrentlyService) processConsumeResult(status listener.ConsumeConcurrentlyStatus,
	context *consumer.ConsumeConcurrentlyContext, consumeRequest *consumeRequest) {
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
	case heartbeat.BROADCASTING:
		for i := ackIndex + 1; i < len(consumeRequest.msgs); i++ {
			msg := consumeRequest.msgs[i]
			logger.Warnf("BROADCASTING, the message consume failed, drop it, %v", msg.MsgId)
		}
	case heartbeat.CLUSTERING:
		msgBackFailed := []*message.MessageExt{}
		// 主要用于过滤掉失败消息,仅剩下已经成功的,用于后面业务删除
		msgList := []*message.MessageExt{}
		msgSet := set.NewSet()
		// 主要利用set去重功能
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
				msgList = append(msgList, val.(*message.MessageExt))
			}
			consumeRequest.msgs = msgList
			service.submitConsumeRequestLater(msgBackFailed, consumeRequest.processQueue, consumeRequest.messageQueue)
		}
	}
	// 删除已经已经成功消费的消息
	offset := consumeRequest.processQueue.RemoveMessage(consumeRequest.msgs)
	// 更新offset用于持久化
	if offset >= 0 {
		service.defaultMQPushConsumerImpl.OffsetStore.UpdateOffset(consumeRequest.messageQueue, offset, true)
	}

}

func (service *ConsumeMessageConcurrentlyService) submitConsumeRequestLater(msgs []*message.MessageExt, processQueue *consumer.ProcessQueue, messageQueue *message.MessageQueue) {
	go func() {
		time.Sleep(time.Second * 5)
		service.SubmitConsumeRequest(msgs, processQueue, messageQueue, true)
	}()
}

// 提交消费请求
func (service *ConsumeMessageConcurrentlyService) SubmitConsumeRequest(msgs []*message.MessageExt, processQueue *consumer.ProcessQueue, messageQueue *message.MessageQueue, dispathToConsume bool) {
	consumeBatchSize := service.defaultMQPushConsumer.consumeMessageBatchMaxSize
	if len(msgs) <= consumeBatchSize {
		consumeRequest := &consumeRequest{msgs: msgs, processQueue: processQueue, messageQueue: messageQueue, ConsumeMessageConcurrentlyService: service}
		service.consumeExecutor <- 1
		go consumeRequest.run()
	} else {
		for total := 0; total < len(msgs); {
			msgThis := []*message.MessageExt{}
			for i := 0; i < consumeBatchSize; i++ {
				if total < len(msgs) {
					msgThis = append(msgThis, msgs[total])
				} else {
					break
				}
				total++
			}
			consumeRequest := &consumeRequest{msgs: msgThis, processQueue: processQueue, messageQueue: messageQueue, ConsumeMessageConcurrentlyService: service}
			service.consumeExecutor <- 1
			go consumeRequest.run()
		}
	}
}

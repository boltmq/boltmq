package main

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/listener"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"time"
)



type MessageListenerImpl struct {
}


func (listenerImpl *MessageListenerImpl)ConsumeMessage(msgs []message.MessageExt, context consumer.ConsumeConcurrentlyContext) listener.ConsumeConcurrentlyStatus {
	return listener.CONSUME_SUCCESS
}
func taskC() {
	t := time.NewTicker(time.Second * 1000)
	for {
		select {
		case <-t.C:
		}

	}
}

func main() {
	defaultMQPushConsumer := process.NewDefaultMQPushConsumer("myConsumerGroup")
	defaultMQPushConsumer.SetConsumeFromWhere(heartbeat.CONSUME_FROM_LAST_OFFSET)
	defaultMQPushConsumer.SetMessageModel(heartbeat.BROADCASTING)
	defaultMQPushConsumer.SetNamesrvAddr("127.0.0.1:9876")
	defaultMQPushConsumer.Subscribe("TestTopic", "tagA")
	defaultMQPushConsumer.RegisterMessageListener(&MessageListenerImpl{})
	var listener listener.MessageListener = &MessageListenerImpl{}
	var listener1 =listener.(consumer.MessageListenerConcurrently)
	listener1.ConsumeMessage(nil,consumer.ConsumeConcurrentlyContext{})
	defaultMQPushConsumer.Start()
	time.Sleep(time.Second*600)
	defaultMQPushConsumer.Shutdown()
	go taskC()
	select {

	}
}


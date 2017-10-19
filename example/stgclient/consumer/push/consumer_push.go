package main

import (
	"flag"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/listener"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"sync/atomic"
	"time"
)

type MessageListenerImpl struct {
	MsgCount   int64
	StartTime  int64
	MapContent *sync.Map
}

func (listenerImpl *MessageListenerImpl) ConsumeMessage(msgs []*message.MessageExt, context *consumer.ConsumeConcurrentlyContext) listener.ConsumeConcurrentlyStatus {
	for _, msg := range msgs {
		count := atomic.AddInt64(&listenerImpl.MsgCount, 1)
		listenerImpl.MapContent.Put(msg.ToString(), 0)
		var num int64 = 10000
		if count%num == 0 {
			fmt.Println(count, msg.ToString(), listenerImpl.MapContent.Size())
		}
		if count >= 5050000 {
			fmt.Println(count, msg.ToString(), listenerImpl.MapContent.Size())
		}

	}
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

var (
	def_consumerGroupId = "consumerGroupId-200"
	def_topic           = "cloudzone123"
	def_namesrvAddr     = "10.112.68.189:9876"
	def_tag             = "tagA"
)

func main() {
	consumerGroupId := flag.String("p", def_consumerGroupId, "the consumer group id ")
	topic := flag.String("topic", def_topic, "the topic for use")
	tag := flag.String("tag", def_tag, "the tag for use")
	namesrvAddr := flag.String("h", def_namesrvAddr, "the namesrv ip:port")
	flag.Parse()

	defaultMQPushConsumer := process.NewDefaultMQPushConsumer(*consumerGroupId)
	defaultMQPushConsumer.SetConsumeFromWhere(heartbeat.CONSUME_FROM_LAST_OFFSET)
	defaultMQPushConsumer.SetMessageModel(heartbeat.CLUSTERING)
	defaultMQPushConsumer.SetNamesrvAddr(*namesrvAddr)
	defaultMQPushConsumer.Subscribe(*topic, *tag)
	defaultMQPushConsumer.RegisterMessageListener(&MessageListenerImpl{StartTime: time.Now().Unix(), MapContent: sync.NewMap()})
	defaultMQPushConsumer.Start()
	go taskC()
	select {}
}

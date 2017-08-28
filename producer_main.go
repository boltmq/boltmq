package main

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"time"
)

func Task() {
	t := time.NewTicker(time.Second * 1000)
	for {
		select {
		case <-t.C:
		}

	}
}
func main() {
	defaultMQProducer := process.NewDefaultMQProducer("producer")
	defaultMQProducer.SetNamesrvAddr("127.0.0.1:9876")
	defaultMQProducer.Start()
	defaultMQProducer.CreateTopic(stgcommon.DEFAULT_TOPIC, "TestTopic", 8)
	for i := 0; i < 10; i++ {
		sendResult, err := defaultMQProducer.Send(message.NewMessage("TestTopic", "tagA", []byte("I'm so diao!")))
		if err != nil {
			fmt.Println(sendResult.ToString())
		}
	}
	go Task()
	time.Sleep(time.Second * 600)
	defaultMQProducer.Shutdown()
	select {}
}

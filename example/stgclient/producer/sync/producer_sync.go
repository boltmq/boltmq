package main

import (
	"fmt"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)

func TaskSync() {
	t := time.NewTicker(time.Second * 1000)
	for {
		select {
		case <-t.C:
		}

	}
}

func main() {
	defaultMQProducer := process.NewDefaultMQProducer("producer")
	defaultMQProducer.SetNamesrvAddr("10.112.68.189:9876")
	defaultMQProducer.Start()
	//defaultMQProducer.CreateTopic(stgcommon.DEFAULT_TOPIC, "cloudzone1", 8)
	for i := 0; i < 8; i++ {
		sendResult, err := defaultMQProducer.Send(message.NewMessage("cloudzone2", "tagA", []byte("I'm so diao!")))
		if err != nil {
			fmt.Println(err)
		}
		if sendResult != nil {
			fmt.Println(sendResult.ToString())
		}
		time.Sleep(100 * time.Millisecond)
	}
	go TaskSync()
	time.Sleep(time.Second * 600)
	defaultMQProducer.Shutdown()
	select {}
}

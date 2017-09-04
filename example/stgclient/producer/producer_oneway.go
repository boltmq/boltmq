package main

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"time"
)

func TaskOneWay() {
	t := time.NewTicker(time.Second * 1000)
	for {
		select {
		case <-t.C:
		}

	}
}
func main() {
	defaultMQProducer := process.NewDefaultMQProducer("producer")
	defaultMQProducer.SetNamesrvAddr("127.0.0.1:10911")
	defaultMQProducer.Start()
	for i := 0; i < 10; i++ {
		err := defaultMQProducer.SendOneWay(message.NewMessage("TestTopic", "tagA", []byte("send oneway msg")))
		if err != nil {
			fmt.Println(err)
		}
	}
	go TaskOneWay()
	time.Sleep(time.Second * 600)
	defaultMQProducer.Shutdown()
	select {}
}

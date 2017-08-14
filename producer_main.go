package main

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"time"
	//"sync/atomic"
	"fmt"
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
	var b int64=10
	var a *int64=&b
	fmt.Println(*a)
	b=11
	fmt.Println(*a)
	*a=12
	fmt.Println(*a)
	fmt.Println(b)
	defaultMQProducer := process.NewDefaultMQProducer("producer")
	defaultMQProducer.SetNamesrvAddr("127.0.0.1:9876")
	defaultMQProducer.Start()
	for i := 0; i < 10; i++ {
		defaultMQProducer.Send(message.NewMessage("TestTopic", "tagA", []byte("I'm so diao!")))
	}
	go Task()
	select {

	}
}


package main

import (
	"fmt"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"strconv"
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
	for i := 0; i < 640; i++ {
		sendResult, err := defaultMQProducer.Send(message.NewMessage("cloudzone999", "tagA", []byte("I'm so diao!呵呵"+strconv.Itoa(i))))
		if err != nil {
			fmt.Println("send msg err: ----> ", err.Error())
			continue
		}
		if sendResult != nil {
			fmt.Println(sendResult.ToString())
		}
	}
	go TaskSync()
	time.Sleep(time.Second * 600)
	defaultMQProducer.Shutdown()
	select {}
}

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
	//defaultMQProducer.CreateTopic(stgcommon.DEFAULT_TOPIC, "cloudzone1", 8)
	for i := 0; i < 64; i++ {
		body := "I'm so diao!" + strconv.Itoa(i)
		msg := message.NewMessage("cloudzone20", "tagA", []byte(body))
		sendResult, err := defaultMQProducer.Send(msg)
		if err != nil {
			fmt.Println(fmt.Sprintf("send msg err: %s", err.Error()))
			continue
		}
		if sendResult != nil {
			fmt.Println(fmt.Sprintf("msg.body=%s, %s", body, sendResult.ToString()))
		}
		time.Sleep(100 * time.Millisecond)
	}
	go TaskSync()
	time.Sleep(time.Second * 600)
	defaultMQProducer.Shutdown()
	select {}
}

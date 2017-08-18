package main

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"time"
	"fmt"
)

func taskPull() {
	t := time.NewTicker(time.Second * 1000)
	for {
		select {
		case <-t.C:
		}

	}
}

func main() {
	defaultMQPullConsumer := process.NewDefaultMQPullConsumer("myConsumerGroup")
	defaultMQPullConsumer.SetNamesrvAddr("127.0.0.1:9876")
	defaultMQPullConsumer.Start()
	//time.Sleep(time.Second*3)
	mqs := defaultMQPullConsumer.FetchSubscribeMessageQueues("TestTopic")
	for _, mq := range mqs {
		pullResult := defaultMQPullConsumer.Pull(mq, "mq", 0, 32)
		for _, msgExt := range pullResult.MsgFoundList {
			fmt.Println(string(msgExt.Body))
		}
	}
	go taskPull()
	select {

	}
}


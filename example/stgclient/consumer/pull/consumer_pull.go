package main

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"time"
)

func main() {
	defaultMQPullConsumer := process.NewDefaultMQPullConsumer("myConsumerGroup")
	defaultMQPullConsumer.SetNamesrvAddr("127.0.0.1:10911")
	defaultMQPullConsumer.Start()

	mqs := defaultMQPullConsumer.FetchSubscribeMessageQueues("TestTopic")
	for _, mq := range mqs {
		pullResult, err := defaultMQPullConsumer.Pull(mq, "tagA", 0, 32)
		if pullResult == nil || err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(pullResult)
			for _, msgExt := range pullResult.MsgFoundList {

				fmt.Println(string(msgExt.Body))
			}
		}
		time.Sleep(time.Second * 600)
	}

	defaultMQPullConsumer.Shutdown()
	select {}
}

package main

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"time"
	"fmt"
)
func main() {
	defaultMQPullConsumer := process.NewDefaultMQPullConsumer("myConsumerGroup")
	defaultMQPullConsumer.SetNamesrvAddr("127.0.0.1:9876")
	defaultMQPullConsumer.Start()

	mqs := defaultMQPullConsumer.FetchSubscribeMessageQueues("TestTopic")
	for _, mq := range mqs {
		pullResult := defaultMQPullConsumer.Pull(mq, "mq", 0, 32)
		for _, msgExt := range pullResult.MsgFoundList {
			fmt.Println(string(msgExt.Body))
		}
	}
	time.Sleep(time.Second*60)
	defaultMQPullConsumer.Shutdown()
	select {

	}
}


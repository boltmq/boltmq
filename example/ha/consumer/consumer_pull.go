package main

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"time"
	"os/signal"
	"syscall"
	"os"
)

func fetchSubscribeMessageQueues(defaultMQPullConsumer *process.DefaultMQPullConsumer, topic, tag string, startOffset int64, maxNum int) {
	mqs := defaultMQPullConsumer.FetchSubscribeMessageQueues(topic)
	for _, mq := range mqs {
		for {
			pullResult, err := defaultMQPullConsumer.Pull(mq, tag, startOffset, maxNum)
			if pullResult == nil || err != nil {
				fmt.Println(err)
				break
			} else {
				if len(pullResult.MsgFoundList) > 0 {
					fmt.Printf("%#v", pullResult)
				}

				for _, msgExt := range pullResult.MsgFoundList {
					fmt.Printf("%s %s \r\n", msgExt.MsgId, string(msgExt.Body))
				}
			}

			startOffset = pullResult.NextBeginOffset

			time.Sleep(1000 * time.Millisecond)
		}
	}
}

func main() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	var (
		consumerGroup       = "myConsumerGroup"
		nameServer          = "127.0.0.1:9876"
		topic               = "test"
		tag                 = "tagA"
		maxNum              = 32
		startOffset   int64 = 0
	)

	defaultMQPullConsumer := process.NewDefaultMQPullConsumer(consumerGroup)
	defaultMQPullConsumer.SetNamesrvAddr(nameServer)
	defaultMQPullConsumer.Start()
	fetchSubscribeMessageQueues(defaultMQPullConsumer, topic, tag, startOffset, maxNum)

	<-signalChan
	defaultMQPullConsumer.Shutdown()
}

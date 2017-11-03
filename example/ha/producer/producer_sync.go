package main

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"strconv"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"os/signal"
	"syscall"
	"os"
)

func main() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	var (
		namesrvAddr     = "127.0.0.1:9876"
		topic           = "test"
		tag             = "tagA"
		producerGroupId = "producerGroupIdExample-200"
		mssageBody      = "I'm so diao!呵呵"
	)

	defaultMQProducer := process.NewDefaultMQProducer(producerGroupId)
	defaultMQProducer.SetNamesrvAddr(namesrvAddr)
	defaultMQProducer.Start()
	defaultMQProducer.CreateTopic(stgcommon.DEFAULT_TOPIC, topic, 1)

	for i := 0; i < 10000; i++ {
		sendResult, err := defaultMQProducer.Send(message.NewMessage(topic, tag, []byte(mssageBody+strconv.Itoa(i))))
		if err != nil {
			fmt.Println("send msg err: ----> ", err.Error())
			continue
		}
		if sendResult != nil {
			fmt.Println(sendResult.ToString())
		}
	}

	<-signalChan
	defaultMQProducer.Shutdown()
}

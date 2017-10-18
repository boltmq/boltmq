package main

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"strconv"
)

var (
	namesrvAddr     = "10.112.68.189:9876"
	topic           = "luoji"
	tag             = "tagA"
	producerGroupId = "producerGroupIdExample-200"
)

func main() {
	defaultMQProducer := process.NewDefaultMQProducer(producerGroupId)
	defaultMQProducer.SetNamesrvAddr(namesrvAddr)
	defaultMQProducer.Start()
	for i := 0; i < 3600; i++ {
		sendResult, err := defaultMQProducer.Send(message.NewMessage(topic, tag, []byte("I'm so diao!呵呵"+strconv.Itoa(i))))
		if err != nil {
			fmt.Println("send msg err: ----> ", err.Error())
			continue
		}
		if sendResult != nil {
			fmt.Println(sendResult.ToString())
		}
	}
	defaultMQProducer.Shutdown()
}

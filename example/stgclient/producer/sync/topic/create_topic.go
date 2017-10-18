package main

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
)

var (
	namesrvAddr     = "127.0.0.1:9876"
	topic           = "jcptExample"
	tag             = "tagA"
	producerGroupId = "producerGroupId-200"
)

func main() {
	defaultMQProducer := process.NewDefaultMQProducer(producerGroupId)
	defaultMQProducer.SetNamesrvAddr(namesrvAddr)
	defaultMQProducer.Start()
	defaultMQProducer.CreateTopic(stgcommon.DEFAULT_TOPIC, topic, 8)
	defaultMQProducer.Shutdown()
}

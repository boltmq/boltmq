package main

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
)

var (
	namesrvAddr     = "10.122.1.200:9876"
	topic           = "luoji"
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

package main

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
)


func main() {
	var (
		topic           = "cloudzone4"
		producerGroupId = "producerGroupId-200"
	)

	defaultMQProducer := process.NewDefaultMQProducer(producerGroupId)
	defaultMQProducer.SetNamesrvAddr("10.112.68.189:9876")
	defaultMQProducer.Start()
	defaultMQProducer.CreateTopic(stgcommon.DEFAULT_TOPIC, topic, 8)
	defaultMQProducer.Shutdown()
}

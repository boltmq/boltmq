package main

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"time"
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
	var (
		topic           = "cloudzone2"
		producerGroupId = "producerGroupId-200"
	)

	defaultMQProducer := process.NewDefaultMQProducer(producerGroupId)
	defaultMQProducer.SetNamesrvAddr("10.112.68.189:9876")
	defaultMQProducer.Start()
	defaultMQProducer.CreateTopic(stgcommon.DEFAULT_TOPIC, topic, 8)
	go TaskSync()
	time.Sleep(time.Second * 10)
	defaultMQProducer.Shutdown()
}

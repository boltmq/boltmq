package main

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgbroker/out"
)

func main() {
	brokerController := stgbroker.CreateBrokerController()
	topicManager := stgbroker.NewTopicConfigManager(brokerController)
	topicManager.Load()
	brokerOuterAPI := out.NewBrokerOuterAPI()
	brokerOuterAPI.RegisterBroker("10.122.1.200:9876", "out", "10.122.2.28:10911",
		"broker-1", "10.122.1.20:10912", 1, topicManager.TopicConfigSerializeWrapper, false, nil)
}

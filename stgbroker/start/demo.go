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
	brokerOuterAPI.RegisterBroker("127.0.0.1:9876", "defaultCluster", "127.0.0.1:10911", "broker-a",
		"127.0.0.1:10912", 0, topicManager.TopicConfigSerializeWrapper, false, []string{})
	select {}
}

package main

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgbroker/out"
)

func main() {
	namesrvAddr := "0.0.0.0:9876"
	brokerController := stgbroker.CreateBrokerController()
	topicManager := stgbroker.NewTopicConfigManager(brokerController)
	topicManager.Load()
	brokerOuterAPI := out.NewBrokerOuterAPI()

	topicConfigWrapper := brokerController.TopicConfigManager.TopicConfigSerializeWrapper
	brokerOuterAPI.RegisterBroker(namesrvAddr, "DefaultCluster", "127.0.0.1:10911", "broker-b",
		"127.0.0.1:10912", 0, topicConfigWrapper, false, []string{})

	select {

	}
}

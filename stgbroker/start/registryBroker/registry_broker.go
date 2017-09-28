package main

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker"
)

func main() {
	namesrvAddr := "0.0.0.0:9876"
	clusterName := "DefaultCluster"
	brokerAddr := "127.0.0.1:10911"
	brokerName := "broker-b"
	haServerAddr := "127.0.0.1:10912"
	brokerId := int64(0)
	oneway := false

	brokerController := stgbroker.CreateBrokerController()
	brokerController.TopicConfigManager.Load()
	topicConfigWrapper := brokerController.TopicConfigManager.TopicConfigSerializeWrapper
	api := brokerController.BrokerOuterAPI
	api.Start()

	api.RegisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName, haServerAddr, brokerId, topicConfigWrapper, oneway, []string{})

	select {}
}

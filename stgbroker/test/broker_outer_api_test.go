package test

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgbroker/out"
	"testing"
)

func TestRegisterBroker(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicManager := stgbroker.NewTopicConfigManager(brokerController)
	topicManager.Load()
	brokerOuterAPI := out.NewBrokerOuterAPI()
	brokerOuterAPI.Start()
	brokerOuterAPI.RegisterBroker("0.0.0.0:9876", "out", "10.122.2.28:10911",
		"broker-1", "10.122.1.20:10912", 1, topicManager.TopicConfigSerializeWrapper, false, nil)
}

func TestUnregisterBroker(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicManager := stgbroker.NewTopicConfigManager(brokerController)
	topicManager.Load()
	brokerOuterAPI := out.NewBrokerOuterAPI()
	brokerOuterAPI.Start()
	brokerOuterAPI.UnregisterBroker("0.0.0.0:9876", "out", "10.122.2.28:10911",
		"broker-1", 1)
}

func TestNameServer(t *testing.T) {
	brokerOuterAPI := out.NewBrokerOuterAPI()
	brokerOuterAPI.Start()
	brokerOuterAPI.UpdateNameServerAddressList("0.0.0.0:9999")
	brokerOuterAPI.FetchNameServerAddr()
}

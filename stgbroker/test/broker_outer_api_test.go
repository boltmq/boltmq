package test

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgbroker/out"
	"testing"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
)

func TestRegisterBroker(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicManager := stgbroker.NewTopicConfigManager(brokerController)
	topicManager.Load()
	brokerOuterAPI := out.NewBrokerOuterAPI(remoting.NewDefalutRemotingClient())
	brokerOuterAPI.Start()
	brokerOuterAPI.RegisterBroker("0.0.0.0:9876", "out", "10.122.2.28:10911",
		"broker-1", "10.122.1.20:10912", 1, topicManager.TopicConfigSerializeWrapper, false, nil)
}

func TestUnRegisterBroker(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicManager := stgbroker.NewTopicConfigManager(brokerController)
	topicManager.Load()
	brokerOuterAPI := out.NewBrokerOuterAPI(remoting.NewDefalutRemotingClient())
	brokerOuterAPI.Start()
	brokerOuterAPI.UnRegisterBroker("0.0.0.0:9876", "out", "10.122.2.28:10911", "broker-1", 1)
}

func TestFetchNameServerAddr(t *testing.T) {
	brokerOuterAPI := out.NewBrokerOuterAPI(remoting.NewDefalutRemotingClient())
	brokerOuterAPI.Start()
	brokerOuterAPI.UpdateNameServerAddressList("0.0.0.0:9999")
	brokerOuterAPI.FetchNameServerAddr()
}

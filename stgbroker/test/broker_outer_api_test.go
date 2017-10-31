package test

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgbroker/out"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"testing"
)

func TestRegisterBroker(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	brokerController.Initialize()
	brokerController.TopicConfigManager.Load()
	topicConfigWrapper := brokerController.TopicConfigManager.TopicConfigSerializeWrapper
	api := brokerController.BrokerOuterAPI
	api.Start()
	api.RegisterBroker("0.0.0.0:9876", "out", "10.122.2.28:10911", "broker-1", "10.122.1.20:10912", 1, topicConfigWrapper, false, nil)
}

func TestUnRegisterBroker(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	brokerController.Initialize()
	brokerController.TopicConfigManager.Load()
	api := brokerController.BrokerOuterAPI
	api.Start()
	api.UnRegisterBroker("0.0.0.0:9876", "out", "10.122.2.28:10911", "broker-1", 1)
}

func TestFetchNameServerAddr(t *testing.T) {
	api := out.NewBrokerOuterAPI(remoting.NewDefalutRemotingClient())
	api.Start()
	api.UpdateNameServerAddressList("0.0.0.0:9999")
	api.FetchNameServerAddr()
}

package stgbroker

import (
	"testing"
	"git.oschina.net/cloudzone/smartgo/stgbroker/out"
	"git.oschina.net/cloudzone/smartgo/stgbroker"
)

func TestRegisterBroker(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicManager:=stgbroker.NewTopicConfigManager(brokerController)
	topicManager.Load()
	brokerOuterAPI := out.NewBrokerOuterAPI()
	brokerOuterAPI.RegisterBroker("0.0.0.0:9876","out","10.122.2.28:10911",
		"broker-1","10.122.1.20:10912",1,topicManager.TopicConfigSerializeWrapper,false,nil)
}

func TestUnregisterBroker(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicManager:=stgbroker.NewTopicConfigManager(brokerController)
	topicManager.Load()
	brokerOuterAPI := out.NewBrokerOuterAPI()
	brokerOuterAPI.UnregisterBroker("0.0.0.0:9876","out","10.122.2.28:10911",
		"broker-1",1)
}

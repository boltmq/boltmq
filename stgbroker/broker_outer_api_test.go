package stgbroker

import (
	"testing"
	"git.oschina.net/cloudzone/smartgo/stgbroker/out"
)

func TestRegisterBroker(t *testing.T) {
	brokerController := CreateBrokerController()
	topicManager:=NewTopicConfigManager(brokerController)
	topicManager.Load()
	brokerOuterAPI := out.NewBrokerOuterAPI()
	brokerOuterAPI.RegisterBroker("0.0.0.0:9876","out","10.122.2.28:10911",
		"broker-1","10.122.1.20:10912",1,topicManager.TopicConfigSerializeWrapper,false,nil)
}

func TestNameRegisterBroker(t *testing.T) {
	//remotingClient:=remoting.NewDefalutRemotingClient()
	//
	//topicManager:=NewTopicConfigManager(brokerController)
	//topicManager.Load()
	//brokerOuterAPI := out.NewBrokerOuterAPI()
	//brokerOuterAPI.RegisterBroker("10.122.1.210:9876","out","10.122.2.28:10911",
	//	"broker-1","10.122.1.20:10912",1,topicManager.TopicConfigSerializeWrapper,false,nil)
}
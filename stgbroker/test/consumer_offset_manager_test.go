package test

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"testing"
)

func TestConsumerOffsetManagerLoad(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	consumerOffsetManager := stgbroker.NewConsumerOffsetManager(brokerController)
	consumerOffsetManager.Load()
	fmt.Println(consumerOffsetManager.Offsets.Size())
}

func TestConsumerOffsetManagerConfigFilePath(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	consumerOffsetManager := stgbroker.NewConsumerOffsetManager(brokerController)
	fmt.Println(consumerOffsetManager.ConfigFilePath())
}

func TestConsumerOffsetManagerQueryOffset(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	consumerOffsetManager := stgbroker.NewConsumerOffsetManager(brokerController)
	consumerOffsetManager.Load()
	fmt.Println(consumerOffsetManager.QueryOffset("SimpleConsumerGroupIdQB-test","TopicTestMQ",3))
}

func TestQueryOffsetByGreoupAndTopic(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	consumerOffsetManager := stgbroker.NewConsumerOffsetManager(brokerController)
	consumerOffsetManager.Load()
	fmt.Println(consumerOffsetManager.QueryOffsetByGreoupAndTopic("SimpleConsumerGroupIdQB-test","TopicTestMQ"))
}

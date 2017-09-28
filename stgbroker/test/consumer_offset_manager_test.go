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

func TestCommitOffset(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	consumerOffsetManager := stgbroker.NewConsumerOffsetManager(brokerController)
	consumerOffsetManager.Load()
	consumerOffsetManager.CommitOffset("SimpleConsumerGroupIdQB-test","TopicTestMQ",3,4)
	consumerOffsetManager.Persist()
}

func TestScanUnsubscribedTopic(t *testing.T) {
		// TODO
}

func TestWhichTopicByConsumer(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	consumerOffsetManager := stgbroker.NewConsumerOffsetManager(brokerController)
	consumerOffsetManager.Load()
	fmt.Println(consumerOffsetManager.WhichGroupByTopic("TopicTestMQ"))
}

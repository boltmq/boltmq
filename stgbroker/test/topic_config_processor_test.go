package test

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"testing"
)

func TestTopicLoad(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicConfigProcessor := stgbroker.NewTopicConfigManager(brokerController)
	topicConfigProcessor.Load()
	fmt.Println(topicConfigProcessor.TopicConfigSerializeWrapper.TopicConfigTable)
}

func TestCreateTopicInSendMessageMethod(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicConfigProcessor := stgbroker.NewTopicConfigManager(brokerController)
	topicConfigProcessor.Load()
	topicConfigProcessor.CreateTopicInSendMessageMethod("TestTopic_222", stgcommon.DEFAULT_TOPIC, "", 4, 0)
	fmt.Println(topicConfigProcessor.TopicConfigSerializeWrapper.TopicConfigTable.Get("TestTopic_222").ReadQueueNums)
}

func TestSelectTopicConfig(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicConfigProcessor := stgbroker.NewTopicConfigManager(brokerController)
	topicConfigProcessor.Load()
	topic := topicConfigProcessor.SelectTopicConfig("TestTopic_SEND")
	fmt.Println(topic.ReadQueueNums)
}

func TestCreateTopicInSendMessageBackMethod(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicConfigProcessor := stgbroker.NewTopicConfigManager(brokerController)
	topicConfigProcessor.CreateTopicInSendMessageBackMethod("TestTopic_110", 5, 4, 0)
	fmt.Println(topicConfigProcessor.TopicConfigSerializeWrapper.TopicConfigTable.Get("TestTopic_110").ReadQueueNums)
}

func TestUpdateTopicConfig(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicConfigProcessor := stgbroker.NewTopicConfigManager(brokerController)
	topicConfig := stgcommon.NewTopicConfig()
	topicConfig.TopicName = "TestTopic_789"
	topicConfig.ReadQueueNums = 3
	topicConfig.WriteQueueNums = 3
	topicConfig.Order = true
	topicConfigProcessor.UpdateTopicConfig(topicConfig)
	fmt.Println(topicConfigProcessor.TopicConfigSerializeWrapper.TopicConfigTable.Get("TestTopic_789").ReadQueueNums)
}

func TestIsOrderTopic(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicConfigProcessor := stgbroker.NewTopicConfigManager(brokerController)
	topicConfigProcessor.Load()
	fmt.Println(topicConfigProcessor.IsOrderTopic("TestTopic_111"))
	fmt.Println(topicConfigProcessor.IsOrderTopic("TestTopic_222"))
}

func TestIsDeleteTopicConfig(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicConfigProcessor := stgbroker.NewTopicConfigManager(brokerController)
	topicConfigProcessor.Load()
	topicConfigProcessor.DeleteTopicConfig("TestTopic_111")
}

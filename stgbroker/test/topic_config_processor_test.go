package test

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"testing"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
)

func TestTopicLoad(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicConfig := stgbroker.NewTopicConfigManager(brokerController)
	topicConfig.Load()
	fmt.Println(topicConfig.TopicConfigSerializeWrapper.TopicConfigTable)
}

func TestCreateTopicInSendMessageMethod(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicConfig := stgbroker.NewTopicConfigManager(brokerController)
	topicConfig.CreateTopicInSendMessageMethod("TestTopic_SEND", stgcommon.DEFAULT_TOPIC,
		"", 4, 0)
	fmt.Println(topicConfig.TopicConfigSerializeWrapper.TopicConfigTable.Get("TestTopic_SEND").ReadQueueNums)
}

func TestSelectTopicConfig(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicConfig := stgbroker.NewTopicConfigManager(brokerController)
	topicConfig.Load()
	topic:=topicConfig.SelectTopicConfig("TestTopic_SEND")
	fmt.Println(topic.ReadQueueNums)
}


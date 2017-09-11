package stgbroker

import (
	"testing"
)


func TestTopicConfig(t *testing.T) {
	brokerController := CreateBrokerController()
	topicConfig := NewTopicConfigManager(brokerController)
	topicConfig.Load()
	topicConfig.configManagerExt.Persist()
}



package stgbroker

import (
	"testing"
	"git.oschina.net/cloudzone/smartgo/stgbroker"
)


func TestTopicConfig(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicConfig := stgbroker.NewTopicConfigManager(brokerController)
	topicConfig.Load()
	topicConfig.ConfigManagerExt.Persist()
}



package test

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"testing"
)

func TestTopicLoad(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	topicConfig := stgbroker.NewTopicConfigManager(brokerController)
	topicConfig.Load()
	fmt.Println(topicConfig.TopicConfigSerializeWrapper.TopicConfigTable)
}

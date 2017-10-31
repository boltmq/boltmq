package test

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"testing"
)

func TestTopicConfigManagerDecode(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	brokerController.Initialize()
	brokerController.TopicConfigManager.Load()
	data := brokerController.TopicConfigManager.Encode(false)
	buf := []byte(data)

	topicWrapper := body.NewTopicConfigSerializeWrapper()
	topicConfigManager := stgbroker.TopicConfigManager{}
	topicConfigManager.TopicConfigSerializeWrapper = topicWrapper
	topicConfigManager.Decode(buf)
	logger.Infof("TopicConfigManager.Decode() success.\n\t\t %s", topicConfigManager.TopicConfigSerializeWrapper.ToString())

}

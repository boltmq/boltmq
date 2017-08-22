package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"os/user"
)

const TOPIC_GROUP_SEPARATOR = "@"

// ConsumerOffsetManager Consumer消费管理
// Author gaoyanlei
// Since 2017/8/9
type ConsumerOffsetManager struct {
	TOPIC_GROUP_SEPARATOR string

	offsetTable *sync.Map

	BrokerController *BrokerController

	configManagerExt     *ConfigManagerExt
}

// NewConsumerOffsetManager 初始化ConsumerOffsetManager
// Author gaoyanlei
// Since 2017/8/9
func NewConsumerOffsetManager(brokerController *BrokerController) *ConsumerOffsetManager {
	var consumerOffsetManager = new(ConsumerOffsetManager)
	consumerOffsetManager.offsetTable = sync.NewMap()
	consumerOffsetManager.TOPIC_GROUP_SEPARATOR = TOPIC_GROUP_SEPARATOR
	consumerOffsetManager.BrokerController = brokerController
	consumerOffsetManager.configManagerExt = NewConfigManagerExt(consumerOffsetManager)
	return consumerOffsetManager
}


func (self *ConsumerOffsetManager) Load() bool{

	return self.configManagerExt.Load()
}

func (self *ConsumerOffsetManager) Encode(prettyFormat bool) string {
	return ""
}

func (self *ConsumerOffsetManager) Decode(jsonString []byte) {
	if len(jsonString) > 0 {

	}
}

func (self *ConsumerOffsetManager) ConfigFilePath() string {
	user, _ := user.Current()
	return GetTopicConfigPath(user.HomeDir)
}

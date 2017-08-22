package stgbroker

import (
	"encoding/json"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"os/user"
)

const TOPIC_GROUP_SEPARATOR = "@"

// ConsumerOffsetManager Consumer消费管理
// Author gaoyanlei
// Since 2017/8/9
type ConsumerOffsetManager struct {
	TOPIC_GROUP_SEPARATOR string

	OffsetTable *sync.Map

	Offsets map[string]map[int]int64 `json:"offsetTable"`

	BrokerController *BrokerController

	configManagerExt *ConfigManagerExt
}

// NewConsumerOffsetManager 初始化ConsumerOffsetManager
// Author gaoyanlei
// Since 2017/8/9
func NewConsumerOffsetManager(brokerController *BrokerController) *ConsumerOffsetManager {
	var consumerOffsetManager = new(ConsumerOffsetManager)
	consumerOffsetManager.OffsetTable = sync.NewMap()
	consumerOffsetManager.TOPIC_GROUP_SEPARATOR = TOPIC_GROUP_SEPARATOR
	consumerOffsetManager.BrokerController = brokerController
	consumerOffsetManager.configManagerExt = NewConfigManagerExt(consumerOffsetManager)
	return consumerOffsetManager
}

func (self *ConsumerOffsetManager) Load() bool {

	return self.configManagerExt.Load()
}

func (self *ConsumerOffsetManager) Encode(prettyFormat bool) string {
	return ""
}

func (self *ConsumerOffsetManager) Decode(jsonString []byte) {
	if len(jsonString) > 0 {
		cc := new(ConsumerOffsetManager)
		json.Unmarshal(jsonString, cc)
		for k, v := range cc.Offsets {
			m :=sync.NewMap()
			for k1, v1 := range v {
				m.Put(k1,v1)
			}
			self.OffsetTable.Put(k, m)
		}
	}
}

func (self *ConsumerOffsetManager) ConfigFilePath() string {
	user, _ := user.Current()
	return GetConsumerOffsetPath(user.HomeDir)
}

func (self *ConsumerOffsetManager) ScanUnsubscribedTopic() {

}

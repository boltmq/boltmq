package stgbroker

import (
	"encoding/json"
	"fmt"
	"os/user"
	"strings"

	"github.com/pquerna/ffjson/ffjson"
)

const TOPIC_GROUP_SEPARATOR = "@"

// ConsumerOffsetManager Consumer消费管理
// Author gaoyanlei
// Since 2017/8/9
type ConsumerOffsetManager struct {
	TOPIC_GROUP_SEPARATOR string

	Offsets *OffsetTable

	BrokerController *BrokerController

	configManagerExt *ConfigManagerExt
}

// NewConsumerOffsetManager 初始化ConsumerOffsetManager
// Author gaoyanlei
// Since 2017/8/9
func NewConsumerOffsetManager(brokerController *BrokerController) *ConsumerOffsetManager {
	var consumerOffsetManager = new(ConsumerOffsetManager)
	consumerOffsetManager.Offsets = newOffsetTable()
	consumerOffsetManager.TOPIC_GROUP_SEPARATOR = TOPIC_GROUP_SEPARATOR
	consumerOffsetManager.BrokerController = brokerController
	consumerOffsetManager.configManagerExt = NewConfigManagerExt(consumerOffsetManager)
	return consumerOffsetManager
}

func (com *ConsumerOffsetManager) Load() bool {

	return com.configManagerExt.Load()
}

func (com *ConsumerOffsetManager) Encode(prettyFormat bool) string {
	fmt.Println(com.Offsets.size())
	if b, err := ffjson.Marshal(com.Offsets); err == nil {
		return string(b)
	}
	return ""
}

func (com *ConsumerOffsetManager) Decode(jsonString []byte) {
	if len(jsonString) > 0 {
		json.Unmarshal(jsonString, com)
		/*
				for k, v := range cc.Offsets {
					m := sync.NewMap()
					for k1, v1 := range v {
						m.Put(k1, v1)
					}
					com.Offsets.put(k, m)
				}
				com.OffsetTable.Put(k, m)
			}
		*/
	}
}

func (com *ConsumerOffsetManager) ConfigFilePath() string {
	user, _ := user.Current()
	return GetConsumerOffsetPath(user.HomeDir)
}

// ScanUnsubscribedTopic 扫描数据被删除了的topic，offset记录也对应删除
// Author gaoyanlei
// Since 2017/8/22
func (com *ConsumerOffsetManager) ScanUnsubscribedTopic() {

	com.Offsets.foreach(func(k string, v map[int]int64) {
		arrays := strings.Split(k, TOPIC_GROUP_SEPARATOR)
		if arrays != nil && len(arrays) == 2 {
			topic := arrays[0]
			fmt.Println(topic)
			group := arrays[1]
			if nil == com.BrokerController.ConsumerManager.FindSubscriptionData(group, topic) {
				//it.Remove()
			}
		}
	})
}

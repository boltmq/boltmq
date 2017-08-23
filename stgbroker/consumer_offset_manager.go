package stgbroker

import (
	"encoding/json"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"os/user"
	"strings"
	"fmt"
	"github.com/pquerna/ffjson/ffjson"
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

func (com *ConsumerOffsetManager) Load() bool {

	return com.configManagerExt.Load()
}

func (com *ConsumerOffsetManager) Encode(prettyFormat bool) string {
	fmt.Println(com.OffsetTable.Size())
	if b, err := ffjson.Marshal(&com.OffsetTable); err == nil {
		return string(b)
	}
	return ""
}

func (com *ConsumerOffsetManager) Decode(jsonString []byte) {
	if len(jsonString) > 0 {
		cc := new(ConsumerOffsetManager)
		json.Unmarshal(jsonString, cc)
		for k, v := range cc.Offsets {
			m := sync.NewMap()
			for k1, v1 := range v {
				m.Put(k1, v1)
			}
			com.OffsetTable.Put(k, m)
		}
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
	for it := com.OffsetTable.Iterator(); it.HasNext(); {
		topicAtGroup, _, _ := it.Next()
		arrays := strings.Split(topicAtGroup.(string), TOPIC_GROUP_SEPARATOR)
		if arrays != nil && len(arrays) == 2 {
			topic := arrays[0]
			fmt.Println(topic)
			group := arrays[1]
			if nil == com.BrokerController.ConsumerManager.FindSubscriptionData(group, topic) {
				//it.Remove()
			}
		}
	}
}

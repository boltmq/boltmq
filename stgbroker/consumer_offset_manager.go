package stgbroker

import (
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
	if b, err := ffjson.Marshal(com.Offsets); err == nil {
		return string(b)
	}
	return ""
}

func (com *ConsumerOffsetManager) Decode(jsonString []byte) {
	if len(jsonString) > 0 {
		fmt.Println(string(jsonString))
		ffjson.Unmarshal(jsonString, com.Offsets)
		fmt.Println(com.Offsets.size())
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

func (com *ConsumerOffsetManager) queryOffset(group, topic string, queueId int) int64 {
	key := topic + TOPIC_GROUP_SEPARATOR + group
	value := com.Offsets.get(key)
	if nil != value {
		offset := value[queueId]
		if offset != 0 {

			return offset
		}
	}
	return -1
}

func (com *ConsumerOffsetManager) CommitOffset(group, topic string, queueId int, offset int64) {
	key := topic + TOPIC_GROUP_SEPARATOR + group
	com.commitOffset(key, queueId, offset)
}

func (com *ConsumerOffsetManager) commitOffset(key string, queueId int, offset int64) {
	value := com.Offsets.get(key)
	if value == nil {
		table := make(map[int]int64)
		table[queueId] = offset
		com.Offsets.put(key, table)
	} else {
		value[queueId] = offset
	}
}

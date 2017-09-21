package stgbroker

import (
	"fmt"
	"os/user"
	"strings"

	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	set "github.com/deckarep/golang-set"
	"github.com/pquerna/ffjson/ffjson"
	"sync"
)

const (
	TOPIC_GROUP_SEPARATOR = "@"
	MAX_VALUE             = 0x7fffffffffffffff
)

// ConsumerOffsetManager Consumer消费管理
// Author gaoyanlei
// Since 2017/8/9
type ConsumerOffsetManager struct {
	TOPIC_GROUP_SEPARATOR string

	Offsets *OffsetTable

	BrokerController *BrokerController

	configManagerExt *ConfigManagerExt

	persistLock *sync.RWMutex
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
		ffjson.Unmarshal(jsonString, com.Offsets)
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

	com.Offsets.Foreach(func(k string, v map[int]int64) {
		arrays := strings.Split(k, TOPIC_GROUP_SEPARATOR)
		if arrays != nil && len(arrays) == 2 {
			topic := arrays[0]
			group := arrays[1]
			if nil == com.BrokerController.ConsumerManager.FindSubscriptionData(group, topic) &&
				com.offsetBehindMuchThanData(topic, v) {
				com.Offsets.Remove(k)
				logger.Warnf("remove topic offset, %s", topic)
			}
		}
	})
}

func (com *ConsumerOffsetManager) queryOffset(group, topic string, queueId int) int64 {
	key := topic + TOPIC_GROUP_SEPARATOR + group
	value := com.Offsets.Get(key)
	if nil != value {
		offset := value[queueId]
		if offset != 0 {

			return offset
		}
	}
	return -1
}

// queryOffset2 获取偏移量
// Author rongzhihong
// Since 2017/9/12
func (com *ConsumerOffsetManager) queryOffset2(group, topic string) map[int]int64 {
	key := topic + TOPIC_GROUP_SEPARATOR + group
	offsetTable := com.Offsets.Get(key)
	return offsetTable
}

func (com *ConsumerOffsetManager) CommitOffset(group, topic string, queueId int, offset int64) {
	key := topic + TOPIC_GROUP_SEPARATOR + group
	com.commitOffset(key, queueId, offset)
}

func (com *ConsumerOffsetManager) commitOffset(key string, queueId int, offset int64) {
	value := com.Offsets.Get(key)
	if value == nil {
		table := make(map[int]int64)
		table[queueId] = offset
		com.Offsets.Put(key, table)
	} else {
		value[queueId] = offset
	}
}

// persist 将内存数据刷入文件中
// Author rongzhihong
// Since 2017/9/12
func (com *ConsumerOffsetManager) persist() {
	defer utils.RecoveredFn()

	jsonString := com.Encode(true)
	if jsonString != "" {
		fileName := com.ConfigFilePath()

		com.persistLock.RLock()
		defer com.persistLock.Unlock()

		stgcommon.String2File([]byte(jsonString), fileName)
	}
}

// offsetBehindMuchThanData 检查偏移量与数据是否相差很大
// Author rongzhihong
// Since 2017/9/12
func (com *ConsumerOffsetManager) offsetBehindMuchThanData(topic string, offsetTable map[int]int64) bool {
	result := len(offsetTable) > 0

	for key, offsetInPersist := range offsetTable {
		minOffsetInStore := com.BrokerController.MessageStore.GetMinOffsetInQueue(topic, int32(key))
		fmt.Println(key)
		if offsetInPersist > minOffsetInStore {
			result = false
			break
		}
	}

	return result
}

// WhichTopicByConsumer 获得消费者的Topic
// Author rongzhihong
// Since 2017/9/18
func (com *ConsumerOffsetManager) WhichTopicByConsumer(group string) set.Set {
	topics := set.NewSet()
	for topicAtGroup := range com.Offsets.Offsets {
		arrays := strings.Split(topicAtGroup, TOPIC_GROUP_SEPARATOR)
		if arrays != nil && len(arrays) == 2 {
			if strings.EqualFold(group, arrays[1]) {
				topics.Add(arrays[0])
			}
		}
	}

	return topics
}

// WhichGroupByTopic 获得Topic的消费者
// Author rongzhihong
// Since 2017/9/18
func (com *ConsumerOffsetManager) WhichGroupByTopic(topic string) set.Set {
	groups := set.NewSet()
	for topicAtGroup := range com.Offsets.Offsets {
		arrays := strings.Split(topicAtGroup, TOPIC_GROUP_SEPARATOR)
		if arrays != nil && len(arrays) == 2 {
			if strings.EqualFold(topic, arrays[0]) {
				groups.Add(arrays[1])
			}
		}
	}

	return groups
}

// CloneOffset 克隆偏移量
// Author rongzhihong
// Since 2017/9/18
func (com *ConsumerOffsetManager) CloneOffset(srcGroup, destGroup, topic string) {
	offsets := com.Offsets.Get(topic + TOPIC_GROUP_SEPARATOR + srcGroup)
	if offsets != nil {
		com.Offsets.Put(topic+TOPIC_GROUP_SEPARATOR+destGroup, offsets)
	}
}

// QueryMinOffsetInAllGroup 查询所有组中最小偏移量
// Author rongzhihong
// Since 2017/9/18
func (com *ConsumerOffsetManager) QueryMinOffsetInAllGroup(topic, filterGroups string) map[int]int64 {
	queueMinOffset := make(map[int]int64)

	if !stgcommon.IsBlank(filterGroups) {
		for _, group := range strings.Split(filterGroups, ",") {
			for groupName := range com.Offsets.Offsets {
				if strings.EqualFold(group, strings.Split(groupName, TOPIC_GROUP_SEPARATOR)[1]) {
					com.Offsets.Remove(groupName)
				}
			}
		}
	}

	for topicGroup := range com.Offsets.Offsets {
		topicGroupArr := strings.Split(topicGroup, TOPIC_GROUP_SEPARATOR)
		if strings.EqualFold(topic, topicGroupArr[0]) {
			offsetTable := com.Offsets.Get(topicGroup)
			if offsetTable == nil {
				continue
			}
			for k, v := range offsetTable {
				minOffset := com.BrokerController.MessageStore.GetMinOffsetInQueue(topic, int32(k))
				if v >= minOffset {
					offset, ok := queueMinOffset[k]
					if !ok {
						queueMinOffset[k] = min(MAX_VALUE, v)
					} else {
						queueMinOffset[k] = min(v, offset)
					}
				}
			}
		}
	}
	return queueMinOffset
}

// min int64 的最小值
// Author rongzhihong
// Since 2017/9/18
func min(a, b int64) int64 {
	if a >= b {
		return b
	}
	return a
}

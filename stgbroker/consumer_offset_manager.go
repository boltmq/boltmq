package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	set "github.com/deckarep/golang-set"
	"github.com/pquerna/ffjson/ffjson"
	"strings"
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
	Offsets               *OffsetTable
	BrokerController      *BrokerController
	configManagerExt      *ConfigManagerExt
	persistLock           sync.RWMutex
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
	defer utils.RecoveredFn()

	if buf, err := ffjson.Marshal(com.Offsets); err == nil {
		return string(buf)
	}
	return ""
}

func (com *ConsumerOffsetManager) Decode(buf []byte) {
	if len(buf) > 0 {
		ffjson.Unmarshal(buf, com.Offsets)
	}
}

func (com *ConsumerOffsetManager) ConfigFilePath() string {
	homeDir := stgcommon.GetUserHomeDir()
	if com.BrokerController.BrokerConfig.StorePathRootDir != "" {
		homeDir = com.BrokerController.BrokerConfig.StorePathRootDir
	}
	return GetConsumerOffsetPath(homeDir)
}

// ScanUnsubscribedTopic 扫描被删除Topic，并删除该Topic对应的Offset
// Author gaoyanlei
// Since 2017/8/22
func (self *ConsumerOffsetManager) ScanUnsubscribedTopic() {
	self.Offsets.RemoveByFlag(func(k string, v map[int]int64) bool {
		arrays := strings.Split(k, TOPIC_GROUP_SEPARATOR)
		if arrays == nil || len(arrays) != 2 {
			return false
		}
		topic := arrays[0]
		group := arrays[1]
		findSubscriptionData := self.BrokerController.ConsumerManager.FindSubscriptionData(group, topic)
		hasBehindMuchThanData := self.offsetBehindMuchThanData(topic, v)

		// 当前订阅关系里面没有group-topic订阅关系（消费端当前是停机的状态）并且offset落后很多,则删除消费进度
		if findSubscriptionData == nil && hasBehindMuchThanData {
			logger.Warnf("remove topic offset, %s", topic)
			return true
		}
		return false
	})
}

// QueryOffset 获取group下topic queueId 的offset
// Author gaoyanlei
// Since 2017/9/10
func (com *ConsumerOffsetManager) QueryOffset(group, topic string, queueId int) int64 {
	key := topic + TOPIC_GROUP_SEPARATOR + group
	value := com.Offsets.Get(key)
	if nil != value {
		offset, ok := value[queueId]
		if ok {
			return offset
		}
	}
	return -1
}

// QueryOffsetByGreoupAndTopic 获取group与topuic所有队列offset
// Author rongzhihong
// Since 2017/9/12
func (com *ConsumerOffsetManager) QueryOffsetByGreoupAndTopic(group, topic string) map[int]int64 {
	key := topic + TOPIC_GROUP_SEPARATOR + group
	offsetTable := com.Offsets.Get(key)
	return offsetTable
}

// CommitOffset 提交offset
// Author gaoyanlei
// Since 2017/9/10
func (com *ConsumerOffsetManager) CommitOffset(group, topic string, queueId int, offset int64) {
	key := topic + TOPIC_GROUP_SEPARATOR + group
	com.commitOffset(key, queueId, offset)
}

func (com *ConsumerOffsetManager) commitOffset(key string, queueId int, offset int64) {
	com.persistLock.Lock()
	defer com.persistLock.Unlock()
	defer utils.RecoveredFn()
	value := com.Offsets.Get(key)
	if value == nil {
		table := make(map[int]int64)
		table[queueId] = offset
		com.Offsets.Put(key, table)
	} else {
		value[queueId] = offset
	}
}

// offsetBehindMuchThanData 检查偏移量与数据是否相差很大
// Author rongzhihong
// Since 2017/9/12
func (com *ConsumerOffsetManager) offsetBehindMuchThanData(topic string, offsetTable map[int]int64) bool {
	result := len(offsetTable) > 0

	for key, offsetInPersist := range offsetTable {
		minOffsetInStore := com.BrokerController.MessageStore.GetMinOffsetInQueue(topic, int32(key))
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

	com.Offsets.Foreach(func(topicAtGroup string, v map[int]int64) {
		topicGroupArray := strings.Split(topicAtGroup, TOPIC_GROUP_SEPARATOR)
		if topicGroupArray != nil && len(topicGroupArray) == 2 {
			if strings.EqualFold(group, topicGroupArray[1]) {
				topics.Add(topicGroupArray[0])
			}
		}
	})

	return topics
}

// WhichGroupByTopic 获得Topic的消费者
// Author rongzhihong
// Since 2017/9/18
func (com *ConsumerOffsetManager) WhichGroupByTopic(topic string) set.Set {
	groups := set.NewSet()

	com.Offsets.Foreach(func(topicAtGroup string, v map[int]int64) {
		topicGroupArray := strings.Split(topicAtGroup, TOPIC_GROUP_SEPARATOR)
		if topicGroupArray != nil && len(topicGroupArray) == 2 {
			if strings.EqualFold(topic, topicGroupArray[0]) {
				groups.Add(topicGroupArray[1])
			}
		}
	})

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
			com.Offsets.RemoveByFlag(func(topicAtGroup string, v map[int]int64) bool {
				topicGroupArr := strings.Split(topicAtGroup, TOPIC_GROUP_SEPARATOR)
				if topicGroupArr != nil && len(topicGroupArr) == 2 {
					if strings.EqualFold(group, topicGroupArr[1]) {
						return true
					}
				}
				return false
			})
		}
	}

	com.Offsets.Foreach(func(topicAtGroup string, offsetTable map[int]int64) {
		topicGroupArr := strings.Split(topicAtGroup, TOPIC_GROUP_SEPARATOR)
		if topicGroupArr == nil || len(topicGroupArr) != 2 {
			return
		}
		if !strings.EqualFold(topic, topicGroupArr[0]) {
			return
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
	})

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

func (com *ConsumerOffsetManager) Persist() {
	com.configManagerExt.Persist()
}

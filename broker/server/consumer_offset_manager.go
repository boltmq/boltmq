// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package server

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/common/logger"
	set "github.com/deckarep/golang-set"
	"github.com/pquerna/ffjson/ffjson"
)

const (
	TOPIC_GROUP_SEPARATOR = "@"
	MAX_VALUE             = 0x7fffffffffffffff
	defaultConfigDir      = "config"
)

// consumerOffsetManager Consumer消费进度管理
// Author gaoyanlei
// Since 2017/8/9
type consumerOffsetManager struct {
	topicGroupSeparator string
	offsets             *OffsetTable
	brokerController    *BrokerController
	cfgManagerLoader    *configManagerLoader
	lock                sync.RWMutex
}

// newConsumerOffsetManager 初始化consumerOffsetManager
// Author gaoyanlei
// Since 2017/8/9
func newConsumerOffsetManager(brokerController *BrokerController) *consumerOffsetManager {
	var com = new(consumerOffsetManager)
	com.offsets = newOffsetTable()
	com.topicGroupSeparator = TOPIC_GROUP_SEPARATOR
	com.brokerController = brokerController
	com.cfgManagerLoader = newConfigManagerLoader(com)
	return com
}

func (com *consumerOffsetManager) load() bool {
	return com.cfgManagerLoader.load()
}

func (com *consumerOffsetManager) encode(prettyFormat bool) string {
	com.lock.RLock()
	defer com.lock.RUnlock()

	if buf, err := ffjson.Marshal(com.offsets); err == nil {
		return string(buf)
	}
	return ""
}

func (com *consumerOffsetManager) decode(buf []byte) {
	com.lock.Lock()
	defer com.lock.Unlock()

	if len(buf) > 0 {
		ffjson.Unmarshal(buf, com.offsets)
	}
}

func (com *consumerOffsetManager) configFilePath() string {
	return fmt.Sprintf("%s%c%s%cconsumerOffset.json", com.brokerController.storeCfg.StorePathRootDir,
		os.PathSeparator, defaultConfigDir, os.PathSeparator)
}

// scanUnsubscribedTopic 扫描被删除Topic，并删除该Topic对应的Offset
// Author gaoyanlei
// Since 2017/8/22
func (com *consumerOffsetManager) scanUnsubscribedTopic() {
	com.offsets.RemoveByFlag(func(k string, v map[int]int64) bool {
		arrays := strings.Split(k, TOPIC_GROUP_SEPARATOR)
		if arrays == nil || len(arrays) != 2 {
			return false
		}
		topic := arrays[0]
		group := arrays[1]
		findSubscriptionData := com.brokerController.csmManager.findSubscriptionData(group, topic)
		hasBehindMuchThanData := com.offsetBehindMuchThanData(topic, v)

		// 当前订阅关系里面没有group-topic订阅关系（消费端当前是停机的状态）并且offset落后很多,则删除消费进度
		if findSubscriptionData == nil && hasBehindMuchThanData {
			logger.Warnf("remove topic offset, %s.", topic)
			return true
		}
		return false
	})
}

// queryOffset 获取group下topic queueId 的offset
// Author gaoyanlei
// Since 2017/9/10
func (com *consumerOffsetManager) queryOffset(group, topic string, queueId int) int64 {
	key := topic + TOPIC_GROUP_SEPARATOR + group
	value := com.offsets.Get(key)
	if nil != value {
		offset, ok := value[queueId]
		if ok {
			return offset
		}
	}
	return -1
}

// queryOffsetByGroupAndTopic 获取group与topuic所有队列offset
// Author rongzhihong
// Since 2017/9/12
func (com *consumerOffsetManager) queryOffsetByGroupAndTopic(group, topic string) map[int]int64 {
	key := topic + TOPIC_GROUP_SEPARATOR + group
	offsetTable := com.offsets.Get(key)
	return offsetTable
}

// CommitOffset 提交offset
// Author gaoyanlei
// Since 2017/9/10
func (com *consumerOffsetManager) commitOffset(group, topic string, queueId int, offset int64) {
	key := topic + TOPIC_GROUP_SEPARATOR + group

	com.lock.Lock()
	defer com.lock.Unlock()

	value := com.offsets.Get(key)
	if value == nil {
		table := make(map[int]int64)
		table[queueId] = offset
		com.offsets.Put(key, table)
	} else {
		value[queueId] = offset
	}
}

// offsetBehindMuchThanData 检查偏移量与数据是否相差很大
// Author rongzhihong
// Since 2017/9/12
func (com *consumerOffsetManager) offsetBehindMuchThanData(topic string, offsetTable map[int]int64) bool {
	result := len(offsetTable) > 0

	for key, offsetInPersist := range offsetTable {
		minOffsetInStore := com.brokerController.messageStore.MinOffsetInQueue(topic, int32(key))
		if offsetInPersist > minOffsetInStore {
			result = false
			break
		}
	}

	return result
}

// whichTopicByConsumer 获得消费者的Topic
// Author rongzhihong
// Since 2017/9/18
func (com *consumerOffsetManager) whichTopicByConsumer(group string) set.Set {
	topics := set.NewSet()

	com.offsets.Foreach(func(topicAtGroup string, v map[int]int64) {
		topicGroupArray := strings.Split(topicAtGroup, TOPIC_GROUP_SEPARATOR)
		if topicGroupArray != nil && len(topicGroupArray) == 2 {
			if strings.EqualFold(group, topicGroupArray[1]) {
				topics.Add(topicGroupArray[0])
			}
		}
	})

	return topics
}

// whichGroupByTopic 获得Topic的消费者
// Author rongzhihong
// Since 2017/9/18
func (com *consumerOffsetManager) whichGroupByTopic(topic string) set.Set {
	groups := set.NewSet()

	com.offsets.Foreach(func(topicAtGroup string, v map[int]int64) {
		topicGroupArray := strings.Split(topicAtGroup, TOPIC_GROUP_SEPARATOR)
		if topicGroupArray != nil && len(topicGroupArray) == 2 {
			if strings.EqualFold(topic, topicGroupArray[0]) {
				groups.Add(topicGroupArray[1])
			}
		}
	})

	return groups
}

// cloneOffset 克隆偏移量
// Author rongzhihong
// Since 2017/9/18
func (com *consumerOffsetManager) cloneOffset(srcGroup, destGroup, topic string) {
	offsets := com.offsets.Get(topic + TOPIC_GROUP_SEPARATOR + srcGroup)
	if offsets != nil {
		com.offsets.Put(topic+TOPIC_GROUP_SEPARATOR+destGroup, offsets)
	}
}

// queryMinOffsetInAllGroup 查询所有组中最小偏移量
// Author rongzhihong
// Since 2017/9/18
func (com *consumerOffsetManager) queryMinOffsetInAllGroup(topic, filterGroups string) map[int]int64 {
	queueMinOffset := make(map[int]int64)

	if !common.IsBlank(filterGroups) {
		for _, group := range strings.Split(filterGroups, ",") {
			com.offsets.RemoveByFlag(func(topicAtGroup string, v map[int]int64) bool {
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

	com.offsets.Foreach(func(topicAtGroup string, offsetTable map[int]int64) {
		topicGroupArr := strings.Split(topicAtGroup, TOPIC_GROUP_SEPARATOR)
		if topicGroupArr == nil || len(topicGroupArr) != 2 {
			return
		}
		if !strings.EqualFold(topic, topicGroupArr[0]) {
			return
		}
		for k, v := range offsetTable {
			minOffset := com.brokerController.messageStore.MinOffsetInQueue(topic, int32(k))
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

func (com *consumerOffsetManager) persist() {
	com.cfgManagerLoader.persist()
}

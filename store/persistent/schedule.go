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
package persistent

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/boltmq/common/logger"
)

const (
	SCHEDULE_TOPIC     = "SCHEDULE_TOPIC_XXX"
	FIRST_DELAY_TIME   = int64(1000)
	DELAY_FOR_A_WHILE  = int64(100)
	DELAY_FOR_A_PERIOD = int64(10000)
)

type scheduleMessageService struct {
	delayLevelTable map[int32]int64         // 每个level对应的延时时间
	offsetTable     map[int32]int64         // 延时计算到了哪里
	ticker          *time.Ticker            // 定时器
	messageStore    *PersistentMessageStore // 存储顶层对象
	maxDelayLevel   int32                   // 最大值
}

func newScheduleMessageService(messageStore *PersistentMessageStore) *scheduleMessageService {
	service := &scheduleMessageService{
		delayLevelTable: make(map[int32]int64, 32),
		offsetTable:     make(map[int32]int64, 32),
		messageStore:    messageStore,
	}
	return service
}

func delayLevel2QueueId(delayLevel int32) int32 {
	return delayLevel - 1
}

func (sms *scheduleMessageService) buildRunningStats(stats map[string]string) {
	for key, value := range sms.offsetTable {
		queueId := delayLevel2QueueId(key)
		delayOffset := value
		maxOffset := sms.messageStore.MaxOffsetInQueue(SCHEDULE_TOPIC, queueId)
		statsValue := fmt.Sprintf("%d,%d", delayOffset, maxOffset)
		statsKey := fmt.Sprintf("%s_%d", SCHEDULE_MESSAGE_OFFSET, key)
		stats[statsKey] = statsValue
	}
}

func (sms *scheduleMessageService) encodeOffsetTable() string {
	result, err := json.Marshal(sms.offsetTable)
	if err != nil {
		logger.Infof("schedule message service offset table to json error: %s.", err)
		return ""
	}

	return string(result)
}

func (sms *scheduleMessageService) computeDeliverTimestamp(delayLevel int32, storeTimestamp int64) int64 {
	time, ok := sms.delayLevelTable[delayLevel]
	if ok {
		return time + storeTimestamp
	}

	return storeTimestamp + 1000
}

func (sms *scheduleMessageService) encode() string {
	return sms.encodeOffsetTable()
}

func (sms *scheduleMessageService) load() bool {
	// TODO
	return true
}

func (sms *scheduleMessageService) start() {
	// TODO
}

func (sms *scheduleMessageService) shutdown() {
	if sms.ticker != nil {
		sms.ticker.Stop()
	}
	logger.Info("shutdown schedule message service.")
}

type runningStats int

const (
	COMMIT_LOG_MAX_OFFSET runningStats = iota
	COMMIT_LOG_MIN_OFFSET
	COMMIT_LOG_DISK_RATIO
	CONSUME_QUEUE_DISK_RATIO
	SCHEDULE_MESSAGE_OFFSET
)

func (state runningStats) String() string {
	switch state {
	case COMMIT_LOG_MAX_OFFSET:
		return "commitLogMaxOffset"
	case COMMIT_LOG_MIN_OFFSET:
		return "commitLogMinOffset"
	case COMMIT_LOG_DISK_RATIO:
		return "commitLogDiskRatio"
	case CONSUME_QUEUE_DISK_RATIO:
		return "consumeQueueDiskRatio"
	case SCHEDULE_MESSAGE_OFFSET:
		return "scheduleMessageOffset"
	default:
		return "Unknow"
	}
}

package stgstorelog

import (
	"time"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"encoding/json"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
)

const (
	SCHEDULE_TOPIC     = "SCHEDULE_TOPIC_XXX"
	FIRST_DELAY_TIME   = int64(1000)
	DELAY_FOR_A_WHILE  = int64(100)
	DELAY_FOR_A_PERIOD = int64(10000)
)

type ScheduleMessageService struct {
	delayLevelTable     map[int32]int64      // 每个level对应的延时时间
	offsetTable         map[int32]int64      // 延时计算到了哪里
	ticker              *time.Ticker         // 定时器
	defaultMessageStore *DefaultMessageStore // 存储顶层对象
	maxDelayLevel       int32                // 最大值
}

func NewScheduleMessageService(defaultMessageStore *DefaultMessageStore) *ScheduleMessageService {
	service := &ScheduleMessageService{
		delayLevelTable:     make(map[int32]int64, 32),
		offsetTable:         make(map[int32]int64, 32),
		defaultMessageStore: defaultMessageStore,
	}
	return service
}

func delayLevel2QueueId(delayLevel int32) int32 {
	return delayLevel - 1
}

func (self *ScheduleMessageService) buildRunningStats(stats map[string]string) {
	for key, value := range self.offsetTable {
		queueId := delayLevel2QueueId(key)
		delayOffset := value
		maxOffset := self.defaultMessageStore.GetMaxOffsetInQueue(SCHEDULE_TOPIC, queueId)
		statsValue := fmt.Sprintf("%d,%d", delayOffset, maxOffset)
		statsKey := fmt.Sprintf("%s_%d", stgcommon.SCHEDULE_MESSAGE_OFFSET.String(), key)
		stats[statsKey] = statsValue
	}
}

func (self *ScheduleMessageService) encodeOffsetTable() string {
	result, err := json.Marshal(self.offsetTable)
	if err != nil {
		logger.Info("schedule message service offset table to json error:", err.Error())
		return ""
	}

	return string(result)
}

func (self *ScheduleMessageService) computeDeliverTimestamp(delayLevel int32, storeTimestamp int64) int64 {
	time, ok := self.delayLevelTable[delayLevel]
	if ok {
		return time + storeTimestamp
	}

	return storeTimestamp + 1000
}

func (self *ScheduleMessageService) Encode() string {
	return self.encodeOffsetTable()
}

func (self *ScheduleMessageService) Load() bool {
	// TODO
	return true
}

func (self *ScheduleMessageService) Start() {
	// TODO
}

func (self *ScheduleMessageService) Shutdown() {
	if self.ticker != nil {
		self.ticker.Stop()
	}
	logger.Info("shutdown schedule message service")
}

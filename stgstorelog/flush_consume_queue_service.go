package stgstorelog

import (
	"time"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
)

const (
	RetryTimesOver = 3
)

type FlushConsumeQueueService struct {
	lastFlushTimestamp  int64
	defaultMessageStore *DefaultMessageStore
	stop                bool
}

func NewFlushConsumeQueueService(defaultMessageStore *DefaultMessageStore) *FlushConsumeQueueService {
	return &FlushConsumeQueueService{defaultMessageStore: defaultMessageStore}
}

func (self *FlushConsumeQueueService) doFlush(retryTimes int32) {
	flushConsumeQueueLeastPages := self.defaultMessageStore.MessageStoreConfig.FlushConsumeQueueLeastPages

	if retryTimes == RetryTimesOver {
		flushConsumeQueueLeastPages = 0
	}

	flushConsumeQueueThoroughInterval := self.defaultMessageStore.MessageStoreConfig.FlushConsumeQueueThoroughInterval
	currentTimeMillis := time.Now().Unix()

	var logicsMsgTimestamp int64

	if currentTimeMillis >= (self.lastFlushTimestamp + int64(flushConsumeQueueThoroughInterval)) {
		self.lastFlushTimestamp = currentTimeMillis
		flushConsumeQueueLeastPages = 0
		logicsMsgTimestamp = self.defaultMessageStore.StoreCheckpoint.logicsMsgTimestamp
	}

	tables := self.defaultMessageStore.ConsumeQueueTable
	iter := tables.Iterator()
	times := int(retryTimes)

	for iter.HasNext() {
		result := false
		for i := 0; i < times && !result; i++ {
			_, value, _ := iter.Next()
			consumeQueue := value.(ConsumeQueue)
			consumeQueue.commit(flushConsumeQueueLeastPages)
		}
	}

	if 0 == flushConsumeQueueLeastPages {
		if logicsMsgTimestamp > 0 {
			self.defaultMessageStore.StoreCheckpoint.logicsMsgTimestamp = logicsMsgTimestamp
		}

		self.defaultMessageStore.StoreCheckpoint.flush()
	}

}

func (self *FlushConsumeQueueService) Start() {
	logger.Info("flush consume queue service service started")

	for {
		if self.stop {
			break
		}

		interval := self.defaultMessageStore.MessageStoreConfig.FlushIntervalConsumeQueue
		time.Sleep(time.Millisecond * time.Duration(interval))
		self.doFlush(1)
	}

	// 正常shutdown时，要保证全部刷盘才退出
	self.doFlush(int32(RetryTimesOver))
}

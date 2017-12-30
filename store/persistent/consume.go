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
	"time"

	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/system"
)

const (
	RetryTimesOver = 3
)

type flushConsumeQueueService struct {
	lastFlushTimestamp int64
	stop               bool
	messageStore       *PersistentMessageStore
}

func newFlushConsumeQueueService(messageStore *PersistentMessageStore) *flushConsumeQueueService {
	return &flushConsumeQueueService{messageStore: messageStore}
}

func (fcqs *flushConsumeQueueService) doFlush(retryTimes int32) {
	flushConsumeQueueLeastPages := fcqs.messageStore.config.FlushConsumeQueueLeastPages

	if retryTimes == RetryTimesOver {
		flushConsumeQueueLeastPages = 0
	}

	flushConsumeQueueThoroughInterval := fcqs.messageStore.config.FlushConsumeQueueThoroughInterval
	currentTimeMillis := system.CurrentTimeMillis()

	var logicMsgTimestamp int64

	if currentTimeMillis >= (fcqs.lastFlushTimestamp + int64(flushConsumeQueueThoroughInterval)) {
		fcqs.lastFlushTimestamp = currentTimeMillis
		flushConsumeQueueLeastPages = 0
		logicMsgTimestamp = fcqs.messageStore.steCheckpoint.logicsMsgTimestamp
	}

	tables := fcqs.messageStore.consumeTopicTable
	times := int(retryTimes)

	for _, value := range tables {
		for _, consumeQueue := range value.consumeQueues {
			result := false
			for i := 0; i < times && !result; i++ {
				result = consumeQueue.commit(flushConsumeQueueLeastPages)
			}
		}
	}

	if 0 == flushConsumeQueueLeastPages {
		if logicMsgTimestamp > 0 {
			fcqs.messageStore.steCheckpoint.logicsMsgTimestamp = logicMsgTimestamp
		}

		fcqs.messageStore.steCheckpoint.flush()
	}
}

func (fcqs *flushConsumeQueueService) start() {
	logger.Info("flush consume queue service started")

	for {
		if fcqs.stop {
			break
		}

		interval := fcqs.messageStore.config.FlushIntervalConsumeQueue
		time.Sleep(time.Millisecond * time.Duration(interval))
		fcqs.doFlush(1)
	}
}

func (fcqs *flushConsumeQueueService) shutdown() {
	fcqs.stop = true

	// 正常shutdown时，要保证全部刷盘才退出
	fcqs.doFlush(int32(RetryTimesOver))
	logger.Info("flush consume queue service end")
}

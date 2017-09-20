package stgstorelog

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"time"
)

const (
	FlushRetryTimesOver = 3
)

type FlushRealTimeService struct {
	lastFlushTimestamp int32
	printTimes         int32
	commitLog          *CommitLog
	stopChan           chan bool
}

func NewFlushRealTimeService(commitLog *CommitLog) *FlushRealTimeService {
	fts := new(FlushRealTimeService)
	fts.commitLog = commitLog
	fts.stopChan = make(chan bool, 1)
	return fts
}

func (self *FlushRealTimeService) start() {
	logger.Info("flush real time service started")

	interval := self.commitLog.DefaultMessageStore.MessageStoreConfig.FlushIntervalCommitLog
	intervalDuration := time.Millisecond * time.Duration(interval)
	ticker := time.NewTicker(intervalDuration)

	for {
		select {
		case <-ticker.C:
			flushPhysicQueueLeastPages := self.commitLog.DefaultMessageStore.MessageStoreConfig.FlushCommitLogLeastPages
			flushPhysicQueueThoroughInterval := self.commitLog.DefaultMessageStore.MessageStoreConfig.FlushCommitLogThoroughInterval
			printFlushProgress := false

			// Print flush progress
			currentTimeMillis := time.Now().Unix()
			if currentTimeMillis >= int64(self.lastFlushTimestamp+flushPhysicQueueThoroughInterval) {
				self.lastFlushTimestamp = int32(currentTimeMillis)
				flushPhysicQueueLeastPages = 0
				self.printTimes += 1
				printFlushProgress = (self.printTimes % 10) == 0
			}

			if printFlushProgress {
				self.printFlushProgress()
			}

			self.commitLog.MapedFileQueue.commit(flushPhysicQueueLeastPages)
			storeTimestamp := self.commitLog.MapedFileQueue.storeTimestamp
			if storeTimestamp > 0 {
				self.commitLog.DefaultMessageStore.StoreCheckpoint.physicMsgTimestamp = storeTimestamp
			}
		case <-self.stopChan:
			self.destroy()
			return
		}
	}
}

func (self *FlushRealTimeService) printFlushProgress() {
	logger.Info("how much disk fall behind memory, ", self.commitLog.MapedFileQueue.howMuchFallBehind())
}

func (self *FlushRealTimeService) shutdown() {
	self.stopChan <- true
}

func (self *FlushRealTimeService) destroy() {
	// Normal shutdown, to ensure that all the flush before exit
	close(self.stopChan)

	result := false
	for i := 0; i < FlushRetryTimesOver && !result; i++ {
		result = self.commitLog.MapedFileQueue.commit(0)
		if result {
			logger.Info("flush real time service shutdown, retry %d times ok", i+1)
		} else {
			logger.Info("flush real time service shutdown, retry %d times not ok", i+1)
		}
	}

	self.printFlushProgress()
	logger.Info("flush real time service end")
}

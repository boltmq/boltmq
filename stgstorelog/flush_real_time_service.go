package stgstorelog

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"time"
	stgsync "git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"sync"
)

const (
	FlushRetryTimesOver = 3
)

type FlushRealTimeService struct {
	lastFlushTimestamp int64
	printTimes         int64
	commitLog          *CommitLog
	notify             *stgsync.Notify
	hasNotified        bool
	stoped             bool
	mutex              *sync.Mutex
}

func NewFlushRealTimeService(commitLog *CommitLog) *FlushRealTimeService {
	fts := new(FlushRealTimeService)
	fts.lastFlushTimestamp = 0
	fts.printTimes = 0
	fts.commitLog = commitLog
	fts.notify = stgsync.NewNotify()
	fts.hasNotified = false
	fts.stoped = false
	fts.mutex = new(sync.Mutex)
	return fts
}

func (self *FlushRealTimeService) start() {
	logger.Info("flush real time service started")

	for {
		if self.stoped {
			break
		}

		var (
			flushCommitLogTimed              = self.commitLog.DefaultMessageStore.MessageStoreConfig.FlushCommitLogTimed
			interval                         = self.commitLog.DefaultMessageStore.MessageStoreConfig.FlushIntervalCommitLog
			flushPhysicQueueLeastPages       = self.commitLog.DefaultMessageStore.MessageStoreConfig.FlushCommitLogLeastPages
			flushPhysicQueueThoroughInterval = self.commitLog.DefaultMessageStore.MessageStoreConfig.FlushCommitLogThoroughInterval
			printFlushProgress               = false
			currentTimeMillis                = time.Now().UnixNano() / 1000000
		)

		if currentTimeMillis >= self.lastFlushTimestamp+int64(flushPhysicQueueThoroughInterval) {
			self.lastFlushTimestamp = currentTimeMillis
			flushPhysicQueueLeastPages = 0
			self.printTimes++
			printFlushProgress = self.printTimes%10 == 0
		}

		if flushCommitLogTimed {
			time.Sleep(time.Duration(interval) * time.Millisecond)
		} else {
			self.waitForRunning(int64(interval))
		}

		if printFlushProgress {
			self.printFlushProgress()
		}

		self.commitLog.MapedFileQueue.commit(flushPhysicQueueLeastPages)
		storeTimestamp := self.commitLog.MapedFileQueue.storeTimestamp
		if storeTimestamp > 0 {
			self.commitLog.DefaultMessageStore.StoreCheckpoint.physicMsgTimestamp = storeTimestamp
		}
	}
}

func (self *FlushRealTimeService) printFlushProgress() {
	logger.Info("how much disk fall behind memory, ", self.commitLog.MapedFileQueue.howMuchFallBehind())
}

func (self *FlushRealTimeService) waitForRunning(interval int64) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.hasNotified {
		self.hasNotified = false
		return
	}

	self.notify.WaitTimeout(time.Duration(interval) * time.Millisecond)
	self.hasNotified = false
}

func (self *FlushRealTimeService) wakeup() {
	if !self.hasNotified {
		self.hasNotified = true
		self.notify.Signal()
	}
}

func (self *FlushRealTimeService) destroy() {
	// Normal shutdown, to ensure that all the flush before exit
	result := false
	for i := 0; i < FlushRetryTimesOver && !result; i++ {
		result = self.commitLog.MapedFileQueue.commit(0)
		if result {
			logger.Infof("flush real time service shutdown, retry %d times OK", i+1)
		} else {
			logger.Infof("flush real time service shutdown, retry %d times Not OK", i+1)
		}

	}

	self.printFlushProgress()
	logger.Info("flush real time service end")
}

func (self *FlushRealTimeService) shutdown() {
	self.stoped = true
	self.destroy()
}

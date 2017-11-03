package stgstorelog

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"sync/atomic"
	"time"
	"sync"
)

type ReputMessageService struct {
	defaultMessageStore *DefaultMessageStore
	reputFromOffset     int64
	reputChan           chan bool
	stoped              bool
	mutex               *sync.Mutex
}

func NewReputMessageService(defaultMessageStore *DefaultMessageStore) *ReputMessageService {
	return &ReputMessageService{
		defaultMessageStore: defaultMessageStore,
		reputFromOffset:     int64(0),
		reputChan:           make(chan bool, 1),
		stoped:              false,
		mutex:               new(sync.Mutex),
	}
}

func (self *ReputMessageService) setReputFromOffset(offset int64) {
	self.reputFromOffset = offset
}

func (self *ReputMessageService) doReput() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	doNext := true
	for {
		if !doNext {
			break
		}

		result := self.defaultMessageStore.CommitLog.getData(self.reputFromOffset)
		if result != nil {
			self.reputFromOffset = result.StartOffset

			for readSize := int32(0); readSize < result.Size && doNext; {
				dispatchRequest := self.defaultMessageStore.CommitLog.checkMessageAndReturnSize(
					result.MappedByteBuffer, false, false)
				size := dispatchRequest.msgSize

				if size > 0 {
					self.defaultMessageStore.putDispatchRequest(dispatchRequest)

					self.reputFromOffset += size
					readSize += int32(size)

					storeStatsService := self.defaultMessageStore.StoreStatsService
					timesTotal := storeStatsService.getSinglePutMessageTopicTimesTotal(dispatchRequest.topic)
					sizeTotal := storeStatsService.getSinglePutMessageTopicSizeTotal(dispatchRequest.topic)
					atomic.AddInt64(&timesTotal, 1)
					atomic.AddInt64(&sizeTotal, dispatchRequest.msgSize)
				} else if size == -1 {
					doNext = false
				} else if size == 0 {
					self.reputFromOffset = self.defaultMessageStore.CommitLog.rollNextFile(self.reputFromOffset)
					readSize = result.Size
				}
			}

			result.Release()
		} else {
			doNext = false
		}
	}
}

func (self *ReputMessageService) start() {
	logger.Info("reput message service started")

	for {
		if self.stoped {
			break
		}

		select {
		case <-self.reputChan:
			self.doReput()
		case <-time.After(1000 * time.Millisecond):
			self.doReput()
		}
	}
}

func (self *ReputMessageService) shutdown() {
	self.stoped = true
	close(self.reputChan)
}

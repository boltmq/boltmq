package stgstorelog

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"time"
)

const (
	GroupCommitRequestHighWater = 600000
)

// GroupCommitService
// Author zhoufei
// Since 2017/10/18
type GroupCommitService struct {
	commitLog   *CommitLog
	stoped      bool
	requestChan chan *GroupCommitRequest
	stopChan    chan bool
}

func NewGroupCommitService(commitLog *CommitLog) *GroupCommitService {
	return &GroupCommitService{
		commitLog:   commitLog,
		stoped:      false,
		requestChan: make(chan *GroupCommitRequest, GroupCommitRequestHighWater),
		stopChan:    make(chan bool, 1),
	}
}

func (self *GroupCommitService) putRequest(request *GroupCommitRequest) {
	self.requestChan <- request
}

func (self *GroupCommitService) doCommit(request *GroupCommitRequest) {
	flushOk := false
	for i := 0; i < 2 && !flushOk; i++ {
		flushOk = self.commitLog.MapedFileQueue.committedWhere >= request.nextOffset
		if !flushOk {
			self.commitLog.MapedFileQueue.commit(0)
		}
	}

	request.wakeupCustomer(flushOk)
	close(request.requestChan)

	storeTimestamp := self.commitLog.MapedFileQueue.storeTimestamp
	if storeTimestamp > 0 {
		self.commitLog.DefaultMessageStore.StoreCheckpoint.physicMsgTimestamp = storeTimestamp
	}
}

func (self *GroupCommitService) start() {
	for {
		select {
		case request := <-self.requestChan:
			self.doCommit(request)
		case <-self.stopChan:
			self.destroy()
		}
	}
}

func (self *GroupCommitService) shutdown() {
	self.stopChan <- true
}

func (self *GroupCommitService) destroy() {
	self.stoped = true

	// Under normal circumstances shutdown, wait for the arrival of the request, and then flush
	time.Sleep(10 * time.Millisecond)

	for request := range self.requestChan {
		self.doCommit(request)
	}

	close(self.requestChan)
	close(self.stopChan)

	logger.Info("group commit service end")
}

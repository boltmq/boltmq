package stgstorelog

import (
	"container/list"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"time"
	stgsync "git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"sync"
)

// GroupCommitService
// Author zhoufei
// Since 2017/10/18
type GroupCommitService struct {
	requestsWrite *list.List
	requestsRead  *list.List
	putMutex      *sync.Mutex
	swapMutex     *sync.Mutex
	commitLog     *CommitLog
	stoped        bool
	notify        *stgsync.Notify
	hasNotified   bool
}

func NewGroupCommitService(commitLog *CommitLog) *GroupCommitService {
	return &GroupCommitService{
		requestsWrite: list.New(),
		requestsRead:  list.New(),
		putMutex:      new(sync.Mutex),
		swapMutex:     new(sync.Mutex),
		commitLog:     commitLog,
		stoped:        false,
		notify:        stgsync.NewNotify(),
		hasNotified:   false,
	}
}

func (self *GroupCommitService) putRequest(request *GroupCommitRequest) {
	self.putMutex.Lock()
	defer self.putMutex.Unlock()

	self.requestsWrite.PushBack(request)
	if !self.hasNotified {
		self.hasNotified = true
		self.notify.Signal()
	}
}

func (self *GroupCommitService) swapRequests() {
	tmp := self.requestsWrite
	self.requestsWrite = self.requestsRead
	self.requestsRead = tmp
}

func (self *GroupCommitService) doCommit() {
	if self.requestsRead.Len() > 0 {
		for element := self.requestsRead.Front(); element != nil; element = element.Next() {
			request := element.Value.(*GroupCommitRequest)
			flushOk := false
			for i := 0; i < 2 && !flushOk; i++ {
				flushOk = self.commitLog.MapedFileQueue.committedWhere >= request.nextOffset
				if !flushOk {
					self.commitLog.MapedFileQueue.commit(0)
				}
			}

			request.wakeupCustomer(flushOk)
		}

		storeTimestamp := self.commitLog.MapedFileQueue.storeTimestamp
		if storeTimestamp > 0 {
			self.commitLog.DefaultMessageStore.StoreCheckpoint.physicMsgTimestamp = storeTimestamp
		}

		self.requestsRead.Init()

	} else {
		self.commitLog.MapedFileQueue.commit(0)
	}
}

func (self *GroupCommitService) waitForRunning() {
	self.swapMutex.Lock()
	defer self.swapMutex.Unlock()

	if self.hasNotified {
		self.hasNotified = false
		self.swapRequests()
		return
	}

	self.notify.Wait()
	self.hasNotified = false
	self.swapRequests()
}

func (self *GroupCommitService) start() {
	for {
		if self.stoped {
			break
		}

		self.waitForRunning()
		self.doCommit()
	}
}

func (self *GroupCommitService) shutdown() {
	self.stoped = true

	// Under normal circumstances shutdown, wait for the arrival of the request, and then flush
	time.Sleep(10 * time.Millisecond)

	self.swapRequests()
	self.doCommit()

	logger.Info("group commit service end")
}

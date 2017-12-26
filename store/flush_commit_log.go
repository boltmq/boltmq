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
package store

import (
	"container/list"
	"sync"
	"time"

	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/system"
)

const (
	FlushRetryTimesOver = 3
)

// GroupCommitRequest
// Author zhoufei
// Since 2017/10/18
type GroupCommitRequest struct {
	nextOffset int64
	notify     *system.Notify
	flushOK    bool
}

func NewGroupCommitRequest(nextOffset int64) *GroupCommitRequest {
	request := new(GroupCommitRequest)
	request.nextOffset = 0
	request.notify = system.CreateNotify()
	request.flushOK = false
	return request
}

func (gcreq *GroupCommitRequest) wakeupCustomer(flushOK bool) {
	gcreq.flushOK = flushOK
	gcreq.notify.Signal()
}

func (gcreq *GroupCommitRequest) waitForFlush(timeout int64) bool {
	gcreq.notify.WaitTimeout(time.Duration(timeout) * time.Millisecond)
	return gcreq.flushOK
}

type FlushCommitLogService interface {
	Start()
	Shutdown()
}

// GroupCommitService
// Author zhoufei
// Since 2017/10/18
type GroupCommitService struct {
	requestsWrite *list.List
	requestsRead  *list.List
	putMutex      sync.Mutex
	swapMutex     sync.Mutex
	commitLog     *CommitLog
	stoped        bool
	notify        *system.Notify
	hasNotified   bool
}

func newGroupCommitService(commitLog *CommitLog) *GroupCommitService {
	return &GroupCommitService{
		requestsWrite: list.New(),
		requestsRead:  list.New(),
		commitLog:     commitLog,
		stoped:        false,
		notify:        system.CreateNotify(),
		hasNotified:   false,
	}
}

func (gcs *GroupCommitService) putRequest(request *GroupCommitRequest) {
	gcs.putMutex.Lock()
	defer gcs.putMutex.Unlock()

	gcs.requestsWrite.PushBack(request)
	if !gcs.hasNotified {
		gcs.hasNotified = true
		gcs.notify.Signal()
	}
}

func (gcs *GroupCommitService) swapRequests() {
	tmp := gcs.requestsWrite
	gcs.requestsWrite = gcs.requestsRead
	gcs.requestsRead = tmp
}

func (gcs *GroupCommitService) doCommit() {
	if gcs.requestsRead.Len() > 0 {
		for element := gcs.requestsRead.Front(); element != nil; element = element.Next() {
			request := element.Value.(*GroupCommitRequest)
			flushOk := false
			for i := 0; i < 2 && !flushOk; i++ {
				flushOk = gcs.commitLog.mapedFileQueue.CommittedWhere() >= request.nextOffset
				if !flushOk {
					gcs.commitLog.mapedFileQueue.Commit(0)
				}
			}

			request.wakeupCustomer(flushOk)
		}

		storeTimestamp := gcs.commitLog.mapedFileQueue.StoreTimestamp()
		if storeTimestamp > 0 {
			gcs.commitLog.defaultMessageStore.StoreCheckpoint.physicMsgTimestamp = storeTimestamp
		}

		gcs.requestsRead.Init()

	} else {
		gcs.commitLog.mapedFileQueue.Commit(0)
	}
}

func (gcs *GroupCommitService) waitForRunning() {
	gcs.swapMutex.Lock()
	defer gcs.swapMutex.Unlock()

	if gcs.hasNotified {
		gcs.hasNotified = false
		gcs.swapRequests()
		return
	}

	gcs.notify.Wait()
	gcs.hasNotified = false
	gcs.swapRequests()
}

func (gcs *GroupCommitService) Start() {
	for {
		if gcs.stoped {
			break
		}

		gcs.waitForRunning()
		gcs.doCommit()
	}
}

func (gcs *GroupCommitService) Shutdown() {
	gcs.stoped = true

	// Under normal circumstances shutdown, wait for the arrival of the request, and then flush
	time.Sleep(10 * time.Millisecond)

	gcs.swapRequests()
	gcs.doCommit()

	logger.Info("group commit service end")
}

type FlushRealTimeService struct {
	lastFlushTimestamp int64
	printTimes         int64
	commitLog          *CommitLog
	notify             *system.Notify
	hasNotified        bool
	stoped             bool
	mutex              sync.Mutex
}

func newFlushRealTimeService(commitLog *CommitLog) *FlushRealTimeService {
	fts := new(FlushRealTimeService)
	fts.lastFlushTimestamp = 0
	fts.printTimes = 0
	fts.commitLog = commitLog
	fts.notify = system.CreateNotify()
	fts.hasNotified = false
	fts.stoped = false
	return fts
}

func (fts *FlushRealTimeService) Start() {
	logger.Info("flush real time service started")

	for {
		if fts.stoped {
			break
		}

		var (
			flushCommitLogTimed              = fts.commitLog.defaultMessageStore.config.FlushCommitLogTimed
			interval                         = fts.commitLog.defaultMessageStore.config.FlushIntervalCommitLog
			flushPhysicQueueLeastPages       = fts.commitLog.defaultMessageStore.config.FlushCommitLogLeastPages
			flushPhysicQueueThoroughInterval = fts.commitLog.defaultMessageStore.config.FlushCommitLogThoroughInterval
			printFlushProgress               = false
			currentTimeMillis                = time.Now().UnixNano() / 1000000
		)

		if currentTimeMillis >= fts.lastFlushTimestamp+int64(flushPhysicQueueThoroughInterval) {
			fts.lastFlushTimestamp = currentTimeMillis
			flushPhysicQueueLeastPages = 0
			fts.printTimes++
			printFlushProgress = fts.printTimes%10 == 0
		}

		if flushCommitLogTimed {
			time.Sleep(time.Duration(interval) * time.Millisecond)
		} else {
			fts.waitForRunning(int64(interval))
		}

		if printFlushProgress {
			fts.printFlushProgress()
		}

		fts.commitLog.mapedFileQueue.Commit(flushPhysicQueueLeastPages)
		storeTimestamp := fts.commitLog.mapedFileQueue.StoreTimestamp()
		if storeTimestamp > 0 {
			fts.commitLog.defaultMessageStore.StoreCheckpoint.physicMsgTimestamp = storeTimestamp
		}
	}
}

func (fts *FlushRealTimeService) printFlushProgress() {
	logger.Info("how much disk fall behind memory, ", fts.commitLog.mapedFileQueue.HowMuchFallBehind())
}

func (fts *FlushRealTimeService) waitForRunning(interval int64) {
	fts.mutex.Lock()
	defer fts.mutex.Unlock()

	if fts.hasNotified {
		fts.hasNotified = false
		return
	}

	fts.notify.WaitTimeout(time.Duration(interval) * time.Millisecond)
	fts.hasNotified = false
}

func (fts *FlushRealTimeService) wakeup() {
	if !fts.hasNotified {
		fts.hasNotified = true
		fts.notify.Signal()
	}
}

func (fts *FlushRealTimeService) destroy() {
	// Normal shutdown, to ensure that all the flush before exit
	result := false
	for i := 0; i < FlushRetryTimesOver && !result; i++ {
		result = fts.commitLog.mapedFileQueue.Commit(0)
		if result {
			logger.Infof("flush real time service shutdown, retry %d times OK", i+1)
		} else {
			logger.Infof("flush real time service shutdown, retry %d times Not OK", i+1)
		}

	}

	fts.printFlushProgress()
	logger.Info("flush real time service end")
}

func (fts *FlushRealTimeService) Shutdown() {
	fts.stoped = true
	fts.destroy()
}

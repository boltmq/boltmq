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
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltmq/common/logger"
)

type reputMessageService struct {
	reputFromOffset int64
	reputChan       chan bool
	stoped          bool
	messageStore    *PersistentMessageStore
	mutex           sync.Mutex
}

func newReputMessageService(messageStore *PersistentMessageStore) *reputMessageService {
	return &reputMessageService{
		messageStore:    messageStore,
		reputFromOffset: int64(0),
		reputChan:       make(chan bool, 1),
		stoped:          false,
	}
}

func (rmsg *reputMessageService) notify() {
	if !rmsg.stoped {
		rmsg.reputChan <- true
	}
}

func (rmsg *reputMessageService) setReputFromOffset(offset int64) {
	rmsg.reputFromOffset = offset
}

func (rmsg *reputMessageService) doReput() {
	rmsg.mutex.Lock()
	defer rmsg.mutex.Unlock()

	doNext := true
	for {
		if !doNext {
			break
		}

		result := rmsg.messageStore.clog.getData(rmsg.reputFromOffset)
		if result != nil {
			rmsg.reputFromOffset = result.startOffset

			for readSize := int32(0); readSize < result.size && doNext; {
				dRequest := rmsg.messageStore.clog.checkMessageAndReturnSize(
					result.byteBuffer, false, false)
				size := dRequest.msgSize

				if size > 0 {
					rmsg.messageStore.putDispatchRequest(dRequest)

					rmsg.reputFromOffset += size
					readSize += int32(size)

					storeStatsService := rmsg.messageStore.storeStats
					timesTotal := storeStatsService.GetSinglePutMessageTopicTimesTotal(dRequest.topic)
					sizeTotal := storeStatsService.GetSinglePutMessageTopicSizeTotal(dRequest.topic)
					atomic.AddInt64(&timesTotal, 1)
					atomic.AddInt64(&sizeTotal, dRequest.msgSize)
				} else if size == -1 {
					doNext = false
				} else if size == 0 {
					rmsg.reputFromOffset = rmsg.messageStore.clog.rollNextFile(rmsg.reputFromOffset)
					readSize = result.size
				}
			}

			result.Release()
		} else {
			doNext = false
		}
	}
}

func (rmsg *reputMessageService) start() {
	logger.Info("reput message service started.")

	for {
		if rmsg.stoped {
			break
		}

		select {
		case <-rmsg.reputChan:
			rmsg.doReput()
		case <-time.After(time.Second):
			rmsg.doReput()
			break
		}
	}
}

func (rmsg *reputMessageService) shutdown() {
	rmsg.stoped = true
	close(rmsg.reputChan)
}

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
	"github.com/boltmq/common/sysflag"
)

type dispatchRequest struct {
	topic                     string
	queueId                   int32
	commitLogOffset           int64
	msgSize                   int64
	tagsCode                  int64
	storeTimestamp            int64
	consumeQueueOffset        int64
	keys                      string
	sysFlag                   int32
	preparedTransactionOffset int64
	producerGroup             string
	tranStateTableOffset      int64
}

type dispatchMessageService struct {
	requestsChan chan *dispatchRequest
	requestSize  int32
	closeChan    chan bool
	messageStore *PersistentMessageStore
	mutex        sync.Mutex
	stop         bool
}

func newDispatchMessageService(putMsgIndexHightWater int32, messageStore *PersistentMessageStore) *dispatchMessageService {
	dms := new(dispatchMessageService)
	rate := int32(float64(putMsgIndexHightWater) * 1.5)
	dms.requestsChan = make(chan *dispatchRequest, rate)
	dms.requestSize = 0
	dms.closeChan = make(chan bool, 1)
	dms.messageStore = messageStore

	return dms
}

func (dms *dispatchMessageService) start() {
	logger.Info("dispatch message service started.")

	for {
		select {
		case request := <-dms.requestsChan:
			dms.doDispatch(request)
		case <-dms.closeChan:
			dms.destroy()
			return
		}
	}
}

func (dms *dispatchMessageService) shutdown() {
	dms.closeChan <- true
}

func (dms *dispatchMessageService) destroy() {
	close(dms.closeChan)
	close(dms.requestsChan)
	logger.Info("dispatch message service end.")
}

func (dms *dispatchMessageService) putRequest(request *dispatchRequest) {
	if !dms.stop {
		dms.requestsChan <- request
		atomic.AddInt32(&dms.requestSize, 1)

		dms.messageStore.storeStats.SetDispatchMaxBuffer(int64(atomic.LoadInt32(&dms.requestSize)))

		putMsgIndexHightWater := dms.messageStore.config.PutMsgIndexHightWater
		if atomic.LoadInt32(&dms.requestSize) > putMsgIndexHightWater {
			logger.Infof("message index buffer size %d > high water %d.", atomic.LoadInt32(&dms.requestSize),
				putMsgIndexHightWater)
			time.Sleep(time.Millisecond * 1)
		}
	}

}

func (dms *dispatchMessageService) doDispatch(request *dispatchRequest) {
	atomic.AddInt32(&dms.requestSize, -1)
	tranType := sysflag.GetTransactionValue(int(request.sysFlag))

	switch tranType {
	case sysflag.TransactionNotType:
		fallthrough
	case sysflag.TransactionCommitType:
		dms.messageStore.putMessagePostionInfo(request.topic, request.queueId,
			request.commitLogOffset, request.msgSize, request.tagsCode,
			request.storeTimestamp, request.consumeQueueOffset)
		break
	case sysflag.TransactionPreparedType:
		fallthrough
	case sysflag.TransactionRollbackType:
		break
	}

	// TODO 更新Transaction State Table
	if len(request.producerGroup) > 0 {
		switch tranType {
		case sysflag.TransactionNotType:
			break
		case sysflag.TransactionPreparedType:
			/*
				dms.messageStore.TransactionStateService.appendPreparedTransaction(
					req.commitLogOffset, req.msgSize, req.storeTimestamp/1000, req.producerGroup.hashCode())
			*/
			break
		case sysflag.TransactionCommitType:
			fallthrough
		case sysflag.TransactionRollbackType:
			/*
				dms.messageStore.TransactionStateService.updateTransactionState(
					req.tranStateTableOffset, req.preparedTransactionOffset, req.producerGroup.hashCode(),
					tranType)
			*/
			break
		}
	}

	if dms.messageStore.config.MessageIndexEnable {
		dms.messageStore.idxService.putRequest(request)
	}
}

func (dms *dispatchMessageService) hasRemainMessage() bool {
	// TODO
	return false
}

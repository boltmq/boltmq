package stgstorelog

import (
	"sync"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"sync/atomic"
	"time"
)

func NewDispatchMessageService(putMsgIndexHightWater int32, defaultMessageStore *DefaultMessageStore) *DispatchMessageService {
	dms := new(DispatchMessageService)
	rate := int32(float64(putMsgIndexHightWater) * 1.5)
	dms.requestsChan = make(chan *DispatchRequest, rate)
	dms.requestSize = 0
	dms.closeChan = make(chan bool, 1)
	dms.mutex = new(sync.Mutex)
	dms.defaultMessageStore = defaultMessageStore

	return dms
}

type DispatchMessageService struct {
	requestsChan        chan *DispatchRequest
	requestSize         int32
	closeChan           chan bool
	defaultMessageStore *DefaultMessageStore
	mutex               *sync.Mutex
	stop                bool
}

func (self *DispatchMessageService) Start() {
	logger.Info("dispatch message service started")

	for {
		select {
		case request := <-self.requestsChan:
			self.doDispatch(request)
		case <-self.closeChan:
			self.destroy()
			return
		}
	}
}

func (self *DispatchMessageService) Shutdown() {
	self.closeChan <- true
}

func (self *DispatchMessageService) destroy() {
	close(self.closeChan)
	close(self.requestsChan)
	logger.Info("dispatch message service end")
}

func (self *DispatchMessageService) putRequest(dispatchRequest *DispatchRequest) {
	if !self.stop {
		self.requestsChan <- dispatchRequest
		atomic.AddInt32(&self.requestSize, 1)

		self.defaultMessageStore.StoreStatsService.setDispatchMaxBuffer(int64(atomic.LoadInt32(&self.requestSize)))

		putMsgIndexHightWater := self.defaultMessageStore.MessageStoreConfig.PutMsgIndexHightWater
		if atomic.LoadInt32(&self.requestSize) > putMsgIndexHightWater {
			logger.Infof("Message index buffer size %d > high water %d", atomic.LoadInt32(&self.requestSize),
				putMsgIndexHightWater)
			time.Sleep(time.Millisecond * 1)
		}
	}

}

func (self *DispatchMessageService) doDispatch(dispatchRequest *DispatchRequest) {
	atomic.AddInt32(&self.requestSize, -1)
	tranType := sysflag.GetTransactionValue(int(dispatchRequest.sysFlag))

	switch tranType {
	case sysflag.TransactionNotType:
		fallthrough
	case sysflag.TransactionCommitType:
		self.defaultMessageStore.putMessagePostionInfo(dispatchRequest.topic, dispatchRequest.queueId,
			dispatchRequest.commitLogOffset, dispatchRequest.msgSize, dispatchRequest.tagsCode,
			dispatchRequest.storeTimestamp, dispatchRequest.consumeQueueOffset)
		break
	case sysflag.TransactionPreparedType:
		fallthrough
	case sysflag.TransactionRollbackType:
		break
	}

	// TODO 更新Transaction State Table
	if len(dispatchRequest.producerGroup) > 0 {
		switch tranType {
		case sysflag.TransactionNotType:
			break
		case sysflag.TransactionPreparedType:
			/*
				self.defaultMessageStore.TransactionStateService.appendPreparedTransaction(
					req.commitLogOffset, req.msgSize, req.storeTimestamp/1000, req.producerGroup.hashCode())
			*/
			break
		case sysflag.TransactionCommitType:
			fallthrough
		case sysflag.TransactionRollbackType:
			/*
				self.defaultMessageStore.TransactionStateService.updateTransactionState(
					req.tranStateTableOffset, req.preparedTransactionOffset, req.producerGroup.hashCode(),
					tranType)
			*/
			break
		}
	}

	if self.defaultMessageStore.MessageStoreConfig.MessageIndexEnable {
		self.defaultMessageStore.IndexService.putRequest(dispatchRequest)
	}
}

func (self *DispatchMessageService) hasRemainMessage() bool {
	// TODO
	return false
}

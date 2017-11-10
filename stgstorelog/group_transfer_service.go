package stgstorelog

import (
	"sync/atomic"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"time"
)

// GroupTransferService 同步进度监听服务，如果达到应用层的写入偏移量，则通知应用层该同步已经完成。
// Author zhoufei
// Since 2017/10/18
type GroupTransferService struct {
	haService            *HAService
	requestChan          chan *GroupCommitRequest
	stopChan             chan bool
	stoped               bool
	notifyTransferObject *sync.Notify
	requestsWrite        []*GroupCommitRequest
	requestsRead         []*GroupCommitRequest
}

func NewGroupTransferService(haService *HAService) *GroupTransferService {
	return &GroupTransferService{
		requestChan:          make(chan *GroupCommitRequest, 100),
		notifyTransferObject: sync.NewNotify(),
	}
}

func (self *GroupTransferService) putRequest(request *GroupCommitRequest) {
	self.requestChan <- request
}

func (self *GroupTransferService) doWaitTransfer() {
	select {
	case request := <-self.requestChan:
		transferOK := atomic.LoadInt64(&self.haService.push2SlaveMaxOffset) >= request.nextOffset
		for i := 0; !transferOK && i < 5; i++ {
			self.notifyTransferObject.WaitTimeout(1000 * time.Millisecond)
			transferOK = atomic.LoadInt64(&self.haService.push2SlaveMaxOffset) >= request.nextOffset
		}

		if !transferOK {
			logger.Warn("transfer message to slave timeout, ", request.nextOffset)
		}

		request.wakeupCustomer(transferOK)
	case <-self.stopChan:
		self.stoped = true
		close(self.requestChan)
		close(self.stopChan)
	}
}

func (self *GroupTransferService) notifyTransferSome() {
	self.notifyTransferObject.Signal()
}

func (self *GroupTransferService) start() {
	for {
		if self.stoped {
			break
		}
		self.notifyTransferObject.Wait()
		self.doWaitTransfer()
	}
}

func (self *GroupTransferService) shutdown() {
	self.stopChan <- true
}

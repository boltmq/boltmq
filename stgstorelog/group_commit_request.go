package stgstorelog

import (
	"time"
)

// GroupCommitRequest
// Author zhoufei
// Since 2017/10/18
type GroupCommitRequest struct {
	nextOffset  int64
	flushOK     bool
	requestChan chan bool
}

func NewGroupCommitRequest(nextOffset int64) *GroupCommitRequest {
	request := new(GroupCommitRequest)
	request.nextOffset = nextOffset
	request.flushOK = false
	request.requestChan = make(chan bool, 1)
	return request
}

func (self *GroupCommitRequest) wakeupCustomer(flushOK bool) {
	self.flushOK = flushOK
	self.requestChan <- true
}

func (self *GroupCommitRequest) waitForFlush(timeout int64) bool {
	select {
	case <-self.requestChan:
		break
	case <-time.After(time.Duration(timeout) * time.Millisecond):
		break
	}

	return self.flushOK
}

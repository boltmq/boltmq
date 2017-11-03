package stgstorelog

import "time"

// GroupCommitRequest
// Author zhoufei
// Since 2017/10/18
type GroupCommitRequest struct {
	nextOffset int64
	flushChan  chan bool
	flushOK    bool
}

func NewGroupCommitRequest(nextOffset int64) *GroupCommitRequest {
	return &GroupCommitRequest{
		nextOffset: nextOffset,
		flushChan:  make(chan bool),
		flushOK:    false,
	}
}

func (self *GroupCommitRequest) wakeupCustomer(flushOK bool) {
	self.flushOK = flushOK
	self.flushChan <- flushOK
}

func (self *GroupCommitRequest) waitForFlush(timeout int64) bool {
	select {
	case <-self.flushChan:
		close(self.flushChan)
		return self.flushOK
	case <-time.After(time.Duration(timeout) * time.Millisecond):
		return false
	}
}

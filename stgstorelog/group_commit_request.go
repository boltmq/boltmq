package stgstorelog

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"time"
)

// GroupCommitRequest
// Author zhoufei
// Since 2017/10/18
type GroupCommitRequest struct {
	nextOffset int64
	notify     *sync.Notify
	flushOK    bool
}

func NewGroupCommitRequest(nextOffset int64) *GroupCommitRequest {
	request := new(GroupCommitRequest)
	request.nextOffset = 0
	request.notify = sync.NewNotify()
	request.flushOK = false
	return request
}

func (self *GroupCommitRequest) wakeupCustomer(flushOK bool) {
	self.flushOK = flushOK
	self.notify.Signal()
}

func (self *GroupCommitRequest) waitForFlush(timeout int64) bool {
	self.notify.WaitTimeout(time.Duration(timeout) * time.Millisecond)
	return self.flushOK
}

package stgstorelog

import (
	stgsync "git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"sync"
	"time"
)

// WaitNotifyObject 用来做线程之间异步通知
// Author zhoufei
// Since 2017/10/23
type WaitNotifyObject struct {
	waitingThreadTable map[int64]*stgsync.Notify
	hasNotified        bool
	notify             *stgsync.Notify
	mutex              *sync.Mutex
}

func NewWaitNotifyObject() *WaitNotifyObject {
	return &WaitNotifyObject{
		waitingThreadTable: make(map[int64]*stgsync.Notify),
		hasNotified:        false,
		notify:             stgsync.NewNotify(),
		mutex:              new(sync.Mutex),
	}
}

func (self *WaitNotifyObject) wakeup(interval int64) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if !self.hasNotified {
		self.hasNotified = true
	}
}

func (self *WaitNotifyObject) waitForRunning(interval int64) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.hasNotified {
		self.hasNotified = false
		return
	}

	self.notify.WaitTimeout(time.Duration(interval) * time.Millisecond)
	self.hasNotified = false
}

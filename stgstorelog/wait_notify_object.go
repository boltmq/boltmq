package stgstorelog

import "sync"

// WaitNotifyObject 用来做线程之间异步通知
// Author zhoufei
// Since 2017/10/23
type WaitNotifyObject struct {
	waitingThreadTable map[int64]bool
	hasNotified        bool
	notifyChan         chan bool
	mutex              *sync.Mutex
}

func NewWaitNotifyObject() *WaitNotifyObject {
	return &WaitNotifyObject{
		waitingThreadTable: make(map[int64]bool),
		hasNotified:        false,
		notifyChan:         make(chan bool),
		mutex:              new(sync.Mutex),
	}
}

func (self *WaitNotifyObject) wakeup(interval int64) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if !self.hasNotified {
		self.hasNotified = true
		self.notifyChan <- true
	}
}

func (self *WaitNotifyObject) waitForRunning(interval int64) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.hasNotified {
		self.hasNotified = false
		return
	}


}

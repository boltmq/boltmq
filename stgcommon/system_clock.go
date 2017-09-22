package stgcommon

import (
	"time"
	"sync/atomic"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
)

type SystemClock struct {
	precision int64
	now       int64
	closeChan chan bool
}

func NewSystemClock(precision int64) *SystemClock {
	systemClock := &SystemClock{
		precision: precision,
		now:       time.Now().UnixNano() / 1000000,
		closeChan: make(chan bool, 1),
	}
	return systemClock
}

func (self *SystemClock) scheduleClockUpdating(tick *time.Ticker) {
	for {
		select {
		case <-self.closeChan:
			tick.Stop()
			close(self.closeChan)
			logger.Info("close system clock service")
			return
		case <-tick.C:
			atomic.StoreInt64(&self.now, time.Now().UnixNano()/1000000)
		}
	}
}

func (self *SystemClock) Now() int64 {
	return self.now
}

func (self *SystemClock) Start() {
	tick := time.NewTicker(time.Duration(self.precision) * time.Millisecond)
	self.scheduleClockUpdating(tick)
}

func (self *SystemClock) Shutdown() {
	self.closeChan <- true
}

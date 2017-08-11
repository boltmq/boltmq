package stgcommon

import (
	"sync/atomic"
	"time"
)

type DataVersion struct {
	timestatmp int64
	counter    int64
}

func NewDataVersion() *DataVersion {
	var dataVersion = new(DataVersion)
	dataVersion.timestatmp = time.Now().UnixNano()
	dataVersion.counter = atomic.AddInt64(&dataVersion.counter, 0)
	return dataVersion
}

func (self *DataVersion) AssignNewOne(dataVersion DataVersion) {
	self.timestatmp = dataVersion.timestatmp
	self.counter = dataVersion.counter
}

func (self *DataVersion) NextVersion() {
	self.timestatmp = time.Now().UnixNano()
	self.counter = atomic.AddInt64(&self.counter, 1)
}

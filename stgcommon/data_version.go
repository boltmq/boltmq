package stgcommon

import (
	"sync/atomic"
	"time"
)

type DataVersion struct {
	Timestatmp int64 `json:"Timestatmp"`
	Counter    int64 `json:"Counter"`
}

func NewDataVersion() *DataVersion {
	var dataVersion = new(DataVersion)
	dataVersion.Timestatmp = time.Now().UnixNano()
	dataVersion.Counter = atomic.AddInt64(&dataVersion.Counter, 0)
	return dataVersion
}

func (self *DataVersion) AssignNewOne(dataVersion DataVersion) {
	self.Timestatmp = dataVersion.Timestatmp
	self.Counter = dataVersion.Counter
}

func (self *DataVersion) NextVersion() {
	self.Timestatmp = time.Now().UnixNano()
	self.Counter = atomic.AddInt64(&self.Counter, 1)
}

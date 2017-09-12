package stgcommon

import (
	"encoding/json"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"sync/atomic"
	"time"
)

type DataVersion struct {
	Timestatmp int64 `json:"timestatmp"`
	Counter    int64 `json:"counter"`
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

func (self *DataVersion) ToString() string {
	info := fmt.Sprintf("dataVersion[timestatmp=%d, counter=%d]", self.Timestatmp, self.Counter)
	return info
}

// ToJson DataVersion转json字符串
// Author rongzhihong
// Since 2017/9/8
func (self *DataVersion) ToJson() string {
	byteArray, err := json.Marshal(self)
	if err != nil {
		logger.Error(err)
		return ""
	}
	return string(byteArray)
}

package stgcommon

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"github.com/pquerna/ffjson/ffjson"
	"sync/atomic"
	"time"
)

type DataVersion struct {
	Timestamp int64 `json:"timestamp"`
	Counter   int64 `json:"counter"`
}

func NewDataVersion(timestamp ...int64) *DataVersion {
	dataVersion := new(DataVersion)
	dataVersion.Timestamp = time.Now().UnixNano()
	if timestamp != nil && len(timestamp) > 0 {
		dataVersion.Timestamp = timestamp[0]
	}
	dataVersion.Counter = atomic.AddInt64(&dataVersion.Counter, 0)
	return dataVersion
}

func (self *DataVersion) AssignNewOne(dataVersion DataVersion) {
	self.Timestamp = dataVersion.Timestamp
	self.Counter = dataVersion.Counter
}

func (this *DataVersion) Equals(dataVersion *DataVersion) bool {
	self := this == nil
	param := dataVersion == nil
	if self && param {
		return true
	}
	if (self && !param) || (!self && param) {
		return false
	}
	return this.Timestamp == dataVersion.Timestamp && this.Counter == dataVersion.Counter
}

func (self *DataVersion) NextVersion() {
	self.Timestamp = time.Now().UnixNano()
	self.Counter = atomic.AddInt64(&self.Counter, 1)
}

func (self *DataVersion) ToString() string {
	info := fmt.Sprintf("dataVersion [timestamp=%d, counter=%d]", self.Timestamp, self.Counter)
	return info
}

func (self *DataVersion) ToJson() string {
	buf, err := ffjson.Marshal(self)
	if err == nil {
		return string(buf)
	}
	logger.Errorf("dataVersion[%#v] ffjson.Marshal() err: %s \n", self, err.Error())
	return ""
}

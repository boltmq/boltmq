package routeinfo

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
)

type BrokerLiveInfo struct {
	LastUpdateTimestamp int64
	DataVersion         stgcommon.DataVersion
	Channel             chan int
	HaServerAddr        string
}

func NewBrokerLiveInfo(dataVersion stgcommon.DataVersion, haServerAddr string, channel chan int) *BrokerLiveInfo {
	brokerLiveInfo := BrokerLiveInfo{
		LastUpdateTimestamp: stgcommon.GetCurrentTimeMillis(),
		DataVersion:         dataVersion,
		HaServerAddr:        haServerAddr,
		Channel:             channel,
	}
	return &brokerLiveInfo
}

func (self *BrokerLiveInfo) ToString() string {
	format := "BrokerLiveInfo [lastUpdateTimestamp=%d, dataVersion=%s, channel=%d, haServerAddr=%s]"
	info := fmt.Sprintf(format, self.LastUpdateTimestamp, self.DataVersion.ToString(), self.Channel, self.HaServerAddr)
	return info
}

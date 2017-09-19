package routeinfo

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

type BrokerLiveInfo struct {
	LastUpdateTimestamp int64
	DataVersion         *stgcommon.DataVersion
	Context             netm.Context
	HaServerAddr        string
}

func NewBrokerLiveInfo(dataVersion *stgcommon.DataVersion, haServerAddr string, ctx netm.Context) *BrokerLiveInfo {
	brokerLiveInfo := BrokerLiveInfo{
		LastUpdateTimestamp: stgcommon.GetCurrentTimeMillis(),
		DataVersion:         dataVersion,
		HaServerAddr:        haServerAddr,
		Context:             ctx,
	}
	return &brokerLiveInfo
}

func (self *BrokerLiveInfo) ToString() string {
	format := "BrokerLiveInfo [lastUpdateTimestamp=%d, dataVersion=%s, conn=%s, haServerAddr=%s]"
	info := fmt.Sprintf(format, self.LastUpdateTimestamp, self.DataVersion.ToString(), self.Context, self.HaServerAddr)
	return info
}

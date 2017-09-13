package routeinfo

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"net"
)

type BrokerLiveInfo struct {
	LastUpdateTimestamp int64
	DataVersion         stgcommon.DataVersion
	Conn                net.Conn
	HaServerAddr        string
}

func NewBrokerLiveInfo(dataVersion stgcommon.DataVersion, haServerAddr string, conn net.Conn) *BrokerLiveInfo {
	brokerLiveInfo := BrokerLiveInfo{
		LastUpdateTimestamp: stgcommon.GetCurrentTimeMillis(),
		DataVersion:         dataVersion,
		HaServerAddr:        haServerAddr,
		Conn:                conn,
	}
	return &brokerLiveInfo
}

func (self *BrokerLiveInfo) ToString() string {
	format := "BrokerLiveInfo [lastUpdateTimestamp=%d, dataVersion=%s, conn=%s, haServerAddr=%s]"
	info := fmt.Sprintf(format, self.LastUpdateTimestamp, self.DataVersion.ToString(), self.Conn, self.HaServerAddr)
	return info
}

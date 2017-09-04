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

func (self *BrokerLiveInfo) ToString() string {
	info := "BrokerLiveInfo [lastUpdateTimestamp=%d, dataVersion=%s, channel=%s, haServerAddr=%s]"
	return fmt.Sprintf(info, self.LastUpdateTimestamp, self.DataVersion.ToString(), self.Channel, self.HaServerAddr)
}

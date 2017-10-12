package routeinfo

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

// BrokerLiveInfo 活动broker存储结构
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/29
type BrokerLiveInfo struct {
	LastUpdateTimestamp int64
	DataVersion         *stgcommon.DataVersion
	Context             netm.Context
	HaServerAddr        string
}

// NewBrokerLiveInfo 初始化BrokerLiveInfo
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/29
func NewBrokerLiveInfo(dataVersion *stgcommon.DataVersion, haServerAddr string, ctx netm.Context) *BrokerLiveInfo {
	brokerLiveInfo := BrokerLiveInfo{
		LastUpdateTimestamp: stgcommon.GetCurrentTimeMillis(),
		DataVersion:         dataVersion,
		HaServerAddr:        haServerAddr,
		Context:             ctx,
	}
	return &brokerLiveInfo
}

// ToString 打印BrokerLiveInfo结构体数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/29
func (self *BrokerLiveInfo) ToString() string {
	format := "BrokerLiveInfo {lastUpdateTimestamp=%d, %s, %s, haServerAddr=%s}"
	info := fmt.Sprintf(format, self.LastUpdateTimestamp, self.DataVersion.ToString(), self.Context.ToString(), self.HaServerAddr)
	return info
}

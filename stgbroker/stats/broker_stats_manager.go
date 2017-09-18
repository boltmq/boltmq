package stats

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/stats"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
)

const (
	TOPIC_PUT_NUMS  = "TOPIC_PUT_NUMS"
	TOPIC_PUT_SIZE  = "TOPIC_PUT_SIZE"
	GROUP_GET_NUMS  = "GROUP_GET_NUMS"
	GROUP_GET_SIZE  = "GROUP_GET_SIZE"
	SNDBCK_PUT_NUMS = "SNDBCK_PUT_NUMS"
	BROKER_PUT_NUMS = "BROKER_PUT_NUMS"
	BROKER_GET_NUMS = "BROKER_GET_NUMS"
	GROUP_GET_FALL  = "GROUP_GET_FALL"
)

// BrokerStatsManager broker统计
// Author gaoyanlei
// Since 2017/8/18
type BrokerStatsManager struct {
	clusterName        string
	statsTable         *sync.Map
	momentStatsItemSet *stats.MomentStatsItemSet
}

//
// Author gaoyanlei
// Since 2017/8/18
func NewBrokerStatsManager(clusterName string) *BrokerStatsManager {
	var bs = new(BrokerStatsManager)
	bs.momentStatsItemSet = stats.NewMomentStatsItemSet("GROUP_GET_FALL")
	bs.clusterName = clusterName
	// TODO brokerController.remotingClient=
	//brokerStatsManager.statsTable.Put()
	return bs
}

// Start  BrokerStatsManager启动入口
// Author rongzhihong
// Since 2017/9/12
func (bsm *BrokerStatsManager) Start() {
	// TODO
}

// Start  BrokerStatsManager停止入口
// Author rongzhihong
// Since 2017/9/12
func (bsm *BrokerStatsManager) Shutdown() {
	// TODO
}

// IncSendBackNums  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncSendBackNums(group, topic string) {
	// TODO
}

// IncTopicPutNums  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncTopicPutNums(topic string) {
	// TODO
}

// IncTopicPutSize  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncTopicPutSize(topic string, size int64) {
	// TODO
}

// IncTopicPutNums  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncBrokerPutNums() {
	// TODO
}

// IncGroupGetNums  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncGroupGetNums(group, topic string, incValue int) {
	// TODO
}

// IncGroupGetSize  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncGroupGetSize(group, topic string, incValue int) {
	// TODO
}

// IncBrokerGetNums  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncBrokerGetNums(incValue int) {
	// TODO
}

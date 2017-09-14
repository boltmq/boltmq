package stats

import "git.oschina.net/cloudzone/smartgo/stgcommon/sync"

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
	clusterName string
	statsTable  *sync.Map
	// TODO  private final MomentStatsItemSet momentStatsItemSet = new MomentStatsItemSet(GROUP_GET_FALL, scheduledExecutorService, log);
}

//
// Author gaoyanlei
// Since 2017/8/18
func NewBrokerStatsManager(clusterName string) *BrokerStatsManager {
	var brokerStatsManager = new(BrokerStatsManager)
	// TODO brokerController.remotingClient=
	//brokerStatsManager.statsTable.Put()
	return brokerStatsManager
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

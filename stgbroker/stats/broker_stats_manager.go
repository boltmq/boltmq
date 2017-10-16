package stats

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/stats"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"sync/atomic"
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
	statsTable         map[string]*stats.StatsItemSet // key: 统计维度，如TOPIC_PUT_SIZE等
	momentStatsItemSet *stats.MomentStatsItemSet
}

// NewBrokerStatsManager 初始化
// Author gaoyanlei
// Since 2017/8/18
func NewBrokerStatsManager(clusterName string) *BrokerStatsManager {
	var bs = new(BrokerStatsManager)

	bs.clusterName = clusterName
	bs.statsTable = make(map[string]*stats.StatsItemSet)
	bs.momentStatsItemSet = stats.NewMomentStatsItemSet(GROUP_GET_FALL)
	bs.statsTable[TOPIC_PUT_NUMS] = stats.NewStatsItemSet(TOPIC_PUT_NUMS)
	bs.statsTable[TOPIC_PUT_SIZE] = stats.NewStatsItemSet(TOPIC_PUT_SIZE)
	bs.statsTable[GROUP_GET_NUMS] = stats.NewStatsItemSet(GROUP_GET_NUMS)
	bs.statsTable[GROUP_GET_SIZE] = stats.NewStatsItemSet(GROUP_GET_SIZE)
	bs.statsTable[SNDBCK_PUT_NUMS] = stats.NewStatsItemSet(SNDBCK_PUT_NUMS)
	bs.statsTable[BROKER_PUT_NUMS] = stats.NewStatsItemSet(BROKER_PUT_NUMS)
	bs.statsTable[BROKER_GET_NUMS] = stats.NewStatsItemSet(BROKER_GET_NUMS)

	return bs
}

// Start  BrokerStatsManager启动入口
// Author rongzhihong
// Since 2017/9/12
func (bsm *BrokerStatsManager) Start() {
	bsm.momentStatsItemSet.MomentStatsTaskTickers.Start()

	for _, statsItemSet := range bsm.statsTable {
		statsItemSet.StatsItemTickers.Start()
	}

	logger.Info("BrokerStatsManager start successful")
}

// Start  BrokerStatsManager停止入口
// Author rongzhihong
// Since 2017/9/12
func (bsm *BrokerStatsManager) Shutdown() {
	defer utils.RecoveredFn()

	// 先关闭momentStatsItemSet的定时任务
	bsm.momentStatsItemSet.MomentStatsTaskTickers.Close()

	// 再关闭statsTable中的定时任务
	for _, statsItemSet := range bsm.statsTable {
		statsItemSet.StatsItemTickers.Close()
	}

	logger.Info("BrokerStatsManager shutdown successful")
}

// GetStatsItem  根据statsName、statsKey获得统计数据
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) GetStatsItem(statsName, statsKey string) *stats.StatsItem {
	if statItemSet, ok := bsm.statsTable[statsName]; ok && statItemSet != nil {
		return statItemSet.GetStatsItem(statsKey)
	}
	return nil
}

// IncTopicPutNums  Topic Put次数加1
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncTopicPutNums(topic string) {
	bsm.statsTable[TOPIC_PUT_NUMS].AddValue(topic, 1, 1)
}

// IncTopicPutSize  Topic Put流量增加size
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncTopicPutSize(topic string, size int64) {
	bsm.statsTable[TOPIC_PUT_SIZE].AddValue(topic, size, 1)
}

// IncGroupGetNums  Topic@Group Get消息个数加incValue
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncGroupGetNums(group, topic string, incValue int) {
	bsm.statsTable[GROUP_GET_NUMS].AddValue(topic+"@"+group, int64(incValue), 1)
}

// IncGroupGetSize  Topic@Group Get消息流量增加incValue
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncGroupGetSize(group, topic string, incValue int) {
	bsm.statsTable[GROUP_GET_SIZE].AddValue(topic+"@"+group, int64(incValue), 1)
}

// incBrokerPutNums  broker Put消息次数加1
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncBrokerPutNums() {
	statsItem := bsm.statsTable[BROKER_PUT_NUMS].GetAndCreateStatsItem(bsm.clusterName)
	atomic.AddInt64(&(statsItem.ValueCounter), 1)
}

// IncBrokerGetNums  broker Get消息个数加incValue
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncBrokerGetNums(incValue int) {
	statsItem := bsm.statsTable[BROKER_GET_NUMS].GetAndCreateStatsItem(bsm.clusterName)
	atomic.AddInt64(&(statsItem.ValueCounter), int64(incValue))
}

// IncSendBackNums  Topic@Group 重试Put次数加1
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncSendBackNums(group, topic string) {
	bsm.statsTable[SNDBCK_PUT_NUMS].AddValue(topic+"@"+group, 1, 1)
}

// TpsGroupGetNums  根据 Topic@Group 获得TPS
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) TpsGroupGetNums(group, topic string) float64 {
	return bsm.statsTable[GROUP_GET_NUMS].GetStatsDataInMinute(topic + "@" + group).Tps
}

// RecordDiskFallBehind  记录 QueueId@Topic@Group 的offset落后数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) RecordDiskFallBehind(group, topic string, queueId int32, fallBehind int64) {
	statsKey := fmt.Sprintf("%d@%s@%s", queueId, topic, group)
	atomic.StoreInt64(&(bsm.momentStatsItemSet.GetAndCreateStatsItem(statsKey).ValueCounter), fallBehind)
}

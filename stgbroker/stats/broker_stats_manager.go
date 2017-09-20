package stats

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/stats"
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
	statsTable         map[string]*stats.StatsItemSet
	momentStatsItemSet *stats.MomentStatsItemSet
}

// NewBrokerStatsManager 初始化
// Author gaoyanlei
// Since 2017/8/18
func NewBrokerStatsManager(clusterName string) *BrokerStatsManager {
	var bs = new(BrokerStatsManager)
	bs.momentStatsItemSet = stats.NewMomentStatsItemSet(GROUP_GET_FALL)
	bs.clusterName = clusterName

	/*topicPutNumsSet := stats.NewStatsItemSet()
	topicPutNumsSet.StatsName = TOPIC_PUT_NUMS
	bs.statsTable["TOPIC_PUT_NUMS"] = topicPutNumsSet

	topicPutSizeSet := stats.NewStatsItemSet()
	topicPutNumsSet.StatsName = TOPIC_PUT_SIZE
	bs.statsTable[TOPIC_PUT_SIZE] = topicPutSizeSet

	groupGetNumsSet := stats.NewStatsItemSet()
	topicPutNumsSet.StatsName = GROUP_GET_NUMS
	bs.statsTable[GROUP_GET_NUMS] = groupGetNumsSet

	groupGetSizeSet := stats.NewStatsItemSet()
	topicPutNumsSet.StatsName = GROUP_GET_SIZE
	bs.statsTable[GROUP_GET_SIZE] = groupGetSizeSet

	sndbckPutNumsSet := stats.NewStatsItemSet()
	topicPutNumsSet.StatsName = SNDBCK_PUT_NUMS
	bs.statsTable[SNDBCK_PUT_NUMS] = sndbckPutNumsSet

	brokerPutNumsSet := stats.NewStatsItemSet()
	topicPutNumsSet.StatsName = BROKER_PUT_NUMS
	bs.statsTable[BROKER_PUT_NUMS] = brokerPutNumsSet

	brokerGetNumsSet := stats.NewStatsItemSet()
	topicPutNumsSet.StatsName = BROKER_GET_NUMS
	bs.statsTable[BROKER_GET_NUMS] = brokerGetNumsSet*/
	return bs
}

// Start  BrokerStatsManager启动入口
// Author rongzhihong
// Since 2017/9/12
func (bsm *BrokerStatsManager) Start() {

}

// Start  BrokerStatsManager停止入口
// Author rongzhihong
// Since 2017/9/12
func (bsm *BrokerStatsManager) Shutdown() {

}

// GetStatsItem  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) GetStatsItem(statsName, statsKey string) *stats.StatsItem {
	return bsm.statsTable[statsName].GetStatsItem(statsKey)
}

// IncTopicPutNums  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncTopicPutNums(topic string) {
	bsm.statsTable["TOPIC_PUT_NUMS"].AddValue(topic, 1, 1)
}

// IncTopicPutSize  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncTopicPutSize(topic string, size int64) {
	bsm.statsTable["TOPIC_PUT_SIZE"].AddValue(topic, 1, 1)
}

// IncGroupGetNums  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncGroupGetNums(group, topic string, incValue int) {
	bsm.statsTable["GROUP_GET_NUMS"].AddValue(topic+"@"+group, int64(incValue), 1)
}

// IncGroupGetSize  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncGroupGetSize(group, topic string, incValue int) {
	bsm.statsTable["GROUP_GET_SIZE"].AddValue(topic+"@"+group, int64(incValue), 1)
}

// incBrokerPutNums  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncBrokerPutNums() {
	bsm.statsTable["BROKER_PUT_NUMS"].GetAndCreateStatsItem(bsm.clusterName).ValueCounter += 1
}

// IncBrokerGetNums  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncBrokerGetNums(incValue int) {
	bsm.statsTable["BROKER_GET_NUMS"].GetAndCreateStatsItem(bsm.clusterName).ValueCounter += int64(incValue)
}

// IncSendBackNums  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) IncSendBackNums(group, topic string) {
	bsm.statsTable["SNDBCK_PUT_NUMS"].AddValue(topic+"@"+group, 1, 1)
}

// TpsGroupGetNums  增加数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) TpsGroupGetNums(group, topic string) float64 {
	return bsm.statsTable["GROUP_GET_NUMS"].GetStatsDataInMinute(topic + "@" + group).Tps
}

// RecordDiskFallBehind  记录
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStatsManager) RecordDiskFallBehind(group, topic string, queueId int32, fallBehind int64) {
	statsKey := fmt.Sprintf("%d@%s@%s", queueId, topic, group)
	bsm.momentStatsItemSet.GetAndCreateStatsItem(statsKey).ValueCounter = fallBehind
}

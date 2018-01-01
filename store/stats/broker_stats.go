// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package stats

import (
	"fmt"
	"sync/atomic"

	"github.com/boltmq/common/logger"
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

// BrokerStats broker统计
// Author gaoyanlei
// Since 2017/8/18
type BrokerStats struct {
	clusterName    string
	statsTable     map[string]*StatsItemSet // key: 统计维度，如TOPIC_PUT_SIZE等
	momentStatsSet *MomentStatsItemSet
}

// NewBrokerStats 初始化
// Author gaoyanlei
// Since 2017/8/18
func NewBrokerStats(clusterName string) *BrokerStats {
	var bs = new(BrokerStats)

	bs.clusterName = clusterName
	bs.statsTable = make(map[string]*StatsItemSet)
	bs.momentStatsSet = NewMomentStatsItemSet(GROUP_GET_FALL)
	bs.statsTable[TOPIC_PUT_NUMS] = NewStatsItemSet(TOPIC_PUT_NUMS)
	bs.statsTable[TOPIC_PUT_SIZE] = NewStatsItemSet(TOPIC_PUT_SIZE)
	bs.statsTable[GROUP_GET_NUMS] = NewStatsItemSet(GROUP_GET_NUMS)
	bs.statsTable[GROUP_GET_SIZE] = NewStatsItemSet(GROUP_GET_SIZE)
	bs.statsTable[SNDBCK_PUT_NUMS] = NewStatsItemSet(SNDBCK_PUT_NUMS)
	bs.statsTable[BROKER_PUT_NUMS] = NewStatsItemSet(BROKER_PUT_NUMS)
	bs.statsTable[BROKER_GET_NUMS] = NewStatsItemSet(BROKER_GET_NUMS)

	return bs
}

// Start  BrokerStats启动入口
// Author rongzhihong
// Since 2017/9/12
func (bsm *BrokerStats) Start() {
	bsm.momentStatsSet.allTickers.Start()

	for _, statsItemSet := range bsm.statsTable {
		statsItemSet.allTickers.Start()
	}

	logger.Info("BrokerStats start successful")
}

// Start  BrokerStats停止入口
// Author rongzhihong
// Since 2017/9/12
func (bsm *BrokerStats) Shutdown() {

	// 先关闭momentStatsSet的定时任务
	bsm.momentStatsSet.allTickers.Close()

	// 再关闭statsTable中的定时任务
	for _, statsItemSet := range bsm.statsTable {
		statsItemSet.allTickers.Close()
	}

	logger.Info("BrokerStats shutdown successful")
}

// GetStatsItem  根据statsName、statsKey获得统计数据
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStats) GetStatsItem(statsName, statsKey string) *StatsItem {
	if statItemSet, ok := bsm.statsTable[statsName]; ok && statItemSet != nil {
		return statItemSet.GetStatsItem(statsKey)
	}
	return nil
}

// IncTopicPutNums  Topic Put次数加1
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStats) IncTopicPutNums(topic string) {
	bsm.statsTable[TOPIC_PUT_NUMS].AddValue(topic, 1, 1)
}

// IncTopicPutSize  Topic Put流量增加size
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStats) IncTopicPutSize(topic string, size int64) {
	bsm.statsTable[TOPIC_PUT_SIZE].AddValue(topic, size, 1)
}

// IncGroupGetNums  Topic@Group Get消息个数加incValue
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStats) IncGroupGetNums(group, topic string, incValue int) {
	bsm.statsTable[GROUP_GET_NUMS].AddValue(topic+"@"+group, int64(incValue), 1)
}

// IncGroupGetSize  Topic@Group Get消息流量增加incValue
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStats) IncGroupGetSize(group, topic string, incValue int) {
	bsm.statsTable[GROUP_GET_SIZE].AddValue(topic+"@"+group, int64(incValue), 1)
}

// incBrokerPutNums  broker Put消息次数加1
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStats) IncBrokerPutNums() {
	statsItem := bsm.statsTable[BROKER_PUT_NUMS].GetAndCreateStatsItem(bsm.clusterName)
	atomic.AddInt64(&(statsItem.ValueCounter), 1)
}

// IncBrokerGetNums  broker Get消息个数加incValue
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStats) IncBrokerGetNums(incValue int) {
	statsItem := bsm.statsTable[BROKER_GET_NUMS].GetAndCreateStatsItem(bsm.clusterName)
	atomic.AddInt64(&(statsItem.ValueCounter), int64(incValue))
}

// IncSendBackNums  Topic@Group 重试Put次数加1
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStats) IncSendBackNums(group, topic string) {
	bsm.statsTable[SNDBCK_PUT_NUMS].AddValue(topic+"@"+group, 1, 1)
}

// TpsGroupGetNums  根据 Topic@Group 获得TPS
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStats) TpsGroupGetNums(group, topic string) float64 {
	return bsm.statsTable[GROUP_GET_NUMS].GetStatsDataInMinute(topic + "@" + group).Tps
}

// RecordDiskFallBehind  记录 QueueId@Topic@Group 的offset落后数量
// Author rongzhihong
// Since 2017/9/17
func (bsm *BrokerStats) RecordDiskFallBehind(group, topic string, queueId int32, fallBehind int64) {
	statsKey := fmt.Sprintf("%d@%s@%s", queueId, topic, group)
	atomic.StoreInt64(&(bsm.momentStatsSet.GetAndCreateStatsItem(statsKey).ValueCounter), fallBehind)
}

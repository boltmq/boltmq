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

type BrokerStats interface {
	Start()
	Shutdown()
	GetStatsItem(statsName, statsKey string) *StatsItem
	IncTopicPutNums(topic string)
	IncTopicPutSize(topic string, size int64)
	IncGroupGetNums(group, topic string, incValue int)
	IncGroupGetSize(group, topic string, incValue int)
	IncBrokerPutNums()
	IncBrokerGetNums(incValue int)
	IncSendBackNums(group, topic string)
	TpsGroupGetNums(group, topic string) float64
	RecordDiskFallBehind(group, topic string, queueId int32, fallBehind int64)
}

// brokerStatsService broker统计
// Author gaoyanlei
// Since 2017/8/18
type brokerStatsService struct {
	clusterName    string
	statsTable     map[string]*StatsItemSet // key: 统计维度，如TOPIC_PUT_SIZE等
	momentStatsSet *MomentStatsItemSet
}

func NewBrokerStats(clusterName string) BrokerStats {
	return newBrokerStatsService(clusterName)
}

// newBrokerStatsService 初始化
// Author gaoyanlei
// Since 2017/8/18
func newBrokerStatsService(clusterName string) *brokerStatsService {
	var bs = new(brokerStatsService)

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

// Start  brokerStatsService启动入口
// Author rongzhihong
// Since 2017/9/12
func (bss *brokerStatsService) Start() {
	bss.momentStatsSet.allTickers.Start()

	for _, statsItemSet := range bss.statsTable {
		statsItemSet.allTickers.Start()
	}

	logger.Info("broker stats service start success.")
}

// Start  brokerStatsService停止入口
// Author rongzhihong
// Since 2017/9/12
func (bss *brokerStatsService) Shutdown() {

	// 先关闭momentStatsSet的定时任务
	bss.momentStatsSet.allTickers.Close()

	// 再关闭statsTable中的定时任务
	for _, statsItemSet := range bss.statsTable {
		statsItemSet.allTickers.Close()
	}

	logger.Info("broker stats service shutdown success.")
}

// GetStatsItem  根据statsName、statsKey获得统计数据
// Author rongzhihong
// Since 2017/9/17
func (bss *brokerStatsService) GetStatsItem(statsName, statsKey string) *StatsItem {
	if statItemSet, ok := bss.statsTable[statsName]; ok && statItemSet != nil {
		return statItemSet.GetStatsItem(statsKey)
	}
	return nil
}

// IncTopicPutNums  Topic Put次数加1
// Author rongzhihong
// Since 2017/9/17
func (bss *brokerStatsService) IncTopicPutNums(topic string) {
	bss.statsTable[TOPIC_PUT_NUMS].AddValue(topic, 1, 1)
}

// IncTopicPutSize  Topic Put流量增加size
// Author rongzhihong
// Since 2017/9/17
func (bss *brokerStatsService) IncTopicPutSize(topic string, size int64) {
	bss.statsTable[TOPIC_PUT_SIZE].AddValue(topic, size, 1)
}

// IncGroupGetNums  Topic@Group Get消息个数加incValue
// Author rongzhihong
// Since 2017/9/17
func (bss *brokerStatsService) IncGroupGetNums(group, topic string, incValue int) {
	bss.statsTable[GROUP_GET_NUMS].AddValue(topic+"@"+group, int64(incValue), 1)
}

// IncGroupGetSize  Topic@Group Get消息流量增加incValue
// Author rongzhihong
// Since 2017/9/17
func (bss *brokerStatsService) IncGroupGetSize(group, topic string, incValue int) {
	bss.statsTable[GROUP_GET_SIZE].AddValue(topic+"@"+group, int64(incValue), 1)
}

// incBrokerPutNums  broker Put消息次数加1
// Author rongzhihong
// Since 2017/9/17
func (bss *brokerStatsService) IncBrokerPutNums() {
	statsItem := bss.statsTable[BROKER_PUT_NUMS].GetAndCreateStatsItem(bss.clusterName)
	atomic.AddInt64(&(statsItem.ValueCounter), 1)
}

// IncBrokerGetNums  broker Get消息个数加incValue
// Author rongzhihong
// Since 2017/9/17
func (bss *brokerStatsService) IncBrokerGetNums(incValue int) {
	statsItem := bss.statsTable[BROKER_GET_NUMS].GetAndCreateStatsItem(bss.clusterName)
	atomic.AddInt64(&(statsItem.ValueCounter), int64(incValue))
}

// IncSendBackNums  Topic@Group 重试Put次数加1
// Author rongzhihong
// Since 2017/9/17
func (bss *brokerStatsService) IncSendBackNums(group, topic string) {
	bss.statsTable[SNDBCK_PUT_NUMS].AddValue(topic+"@"+group, 1, 1)
}

// TpsGroupGetNums  根据 Topic@Group 获得TPS
// Author rongzhihong
// Since 2017/9/17
func (bss *brokerStatsService) TpsGroupGetNums(group, topic string) float64 {
	return bss.statsTable[GROUP_GET_NUMS].GetStatsDataInMinute(topic + "@" + group).Tps
}

// RecordDiskFallBehind  记录 QueueId@Topic@Group 的offset落后数量
// Author rongzhihong
// Since 2017/9/17
func (bss *brokerStatsService) RecordDiskFallBehind(group, topic string, queueId int32, fallBehind int64) {
	statsKey := fmt.Sprintf("%d@%s@%s", queueId, topic, group)
	atomic.StoreInt64(&(bss.momentStatsSet.GetAndCreateStatsItem(statsKey).ValueCounter), fallBehind)
}

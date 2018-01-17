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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/system"
)

// MomentStatsItem  MomentStatsItem
// Author rongzhihong
// Since 2017/9/19
type MomentStatsItem struct {
	ValueCounter int64  `json:"valueCounter"`
	StatsName    string `json:"statsName"`
	StatsKey     string `json:"statsKey"`
}

// NewMomentStatsItem  初始化结构体
// Author rongzhihong
// Since 2017/9/19
func NewMomentStatsItem() *MomentStatsItem {
	mom := new(MomentStatsItem)
	mom.ValueCounter = atomic.AddInt64(&mom.ValueCounter, 0)
	return mom
}

// Init  初始化
// Author rongzhihong
// Since 2017/9/19
func (item *MomentStatsItem) Init() {
	// 分钟整点执行
	var diff float64 = float64(system.ComputNextMinutesTimeMillis() - system.CurrentTimeMillis())
	var delay int = int(math.Abs(diff))
	printAtMinutesTicker := system.NewTicker(false, time.Duration(delay)*time.Millisecond, 5*time.Minute, func() {
		item.printAtMinutes()
	})
	printAtMinutesTicker.Start()
}

// printAtMinutes  打印
// Author rongzhihong
// Since 2017/9/19
func (item *MomentStatsItem) printAtMinutes() {
	logger.Infof("[%s] [%s] Stats Every 5 Minutes, Value: %d", item.StatsName, item.StatsKey, item.ValueCounter)
}

// MomentStatsItemSet  QueueId@Topic@Group的offset落后数量的记录集合
// Author rongzhihong
// Since 2017/9/17
type MomentStatsItemSet struct {
	StatsName      string                      `json:"statsName"`
	statsItemTable map[string]*MomentStatsItem `json:"statsItemTable"`
	allTickers     *system.Tickers             // broker统计的定时任务
	sync.RWMutex
}

// NewMomentStatsItemSet 初始化
// Author rongzhihong
// Since 2017/9/17
func NewMomentStatsItemSet(statsName string) *MomentStatsItemSet {
	item := new(MomentStatsItemSet)
	item.StatsName = statsName
	item.statsItemTable = make(map[string]*MomentStatsItem, 128)
	item.allTickers = system.NewTickers()

	item.init()
	return item
}

// GetAndCreateStatsItem  GetAndCreateStatsItem
// Author rongzhihong
// Since 2017/9/19
func (moment *MomentStatsItemSet) GetAndCreateStatsItem(statsKey string) *MomentStatsItem {
	moment.Lock()
	defer moment.Unlock()

	statsItem := moment.statsItemTable[statsKey]
	if nil == statsItem {
		statsItem = NewMomentStatsItem()
		statsItem.StatsName = moment.StatsName
		statsItem.StatsKey = statsKey
		moment.statsItemTable[statsKey] = statsItem
	}
	return statsItem
}

// SetValue  statsKey的数值加value
// Author rongzhihong
// Since 2017/9/19
func (moment *MomentStatsItemSet) SetValue(statsKey string, value int64) {
	statsItem := moment.GetAndCreateStatsItem(statsKey)
	atomic.AddInt64(&(statsItem.ValueCounter), value)
}

// init  init
// Author rongzhihong
// Since 2017/9/19
func (moment *MomentStatsItemSet) init() {
	diffMin := float64(system.ComputNextMinutesTimeMillis() - system.CurrentTimeMillis())
	var delayMin int = int(math.Abs(diffMin))

	moment.allTickers.Register("momentStatsItemSet_printAtMinutesTicker",
		system.NewTicker(false, time.Duration(delayMin)*time.Millisecond, 5*time.Minute, func() { moment.printAtMinutes() }))
}

// printAtMinutes  输出每分钟数据
// Author rongzhihong
// Since 2017/9/19
func (moment *MomentStatsItemSet) printAtMinutes() {
	moment.RLock()
	defer moment.RUnlock()

	for _, momentStatsItem := range moment.statsItemTable {
		momentStatsItem.printAtMinutes()
	}
}

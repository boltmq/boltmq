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

// StatsItem 统计单元
// Author rongzhihong
// Since 2017/9/19
type StatsItem struct {
	ValueCounter int64             `json:"valueCounter"` // 具体的统计值
	TimesCounter int64             `json:"timesCounter"` // 统计次数
	CSListMinute *callSnapshotPlus `json:"csListMinute"` // 最近一分钟内的镜像，数量6，10秒钟采样一次
	CSListHour   *callSnapshotPlus `json:"csListHour"`   // 最近一小时内的镜像，数量6，10分钟采样一次
	CSListDay    *callSnapshotPlus `json:"csListDay"`    // 最近一天内的镜像，数量24，1小时采样一次
	StatsName    string            `json:"statsName"`
	StatsKey     string            `json:"statsKey"`
}

// NewStatsItem 统计单元初始化
// Author rongzhihong
// Since 2017/9/19
func NewStatsItem() *StatsItem {
	statsItem := new(StatsItem)
	statsItem.CSListMinute = newCallSnapshotPlus()
	statsItem.CSListHour = newCallSnapshotPlus()
	statsItem.CSListDay = newCallSnapshotPlus()
	atomic.AddInt64(&(statsItem.ValueCounter), 0)
	atomic.AddInt64(&(statsItem.TimesCounter), 0)
	return statsItem
}

// computeStatsData 计算获得统计数据
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) computeStatsData(csList *callSnapshotPlus) *StatsSnapshot {
	csList.RLock()
	defer csList.RUnlock()

	var (
		tps   float64 = 0.0
		avgpt float64 = 0.0
		sum   int64   = 0
	)

	statsSnapshot := NewStatsSnapshot()

	if csList.Size() > 0 {
		first := csList.Get(0)
		last := csList.Get(csList.Size() - 1)

		sum = last.value - first.value
		if last.timestamp-first.timestamp > 0 {
			tps = float64(sum) * 1000.0 / float64(last.timestamp-first.timestamp)
		}

		timesDiff := last.times - first.times
		if timesDiff > 0 {
			avgpt = float64(sum) / float64(timesDiff)
		}
	}

	statsSnapshot.Sum = sum
	statsSnapshot.Tps = tps
	statsSnapshot.Avgpt = avgpt

	return statsSnapshot
}

// GetStatsDataInDay 获得分钟统计数据
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) GetStatsDataInMinute() *StatsSnapshot {
	return statsItem.computeStatsData(statsItem.CSListMinute)
}

// GetStatsDataInDay 获得小时统计数据
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) GetStatsDataInHour() *StatsSnapshot {
	return statsItem.computeStatsData(statsItem.CSListHour)
}

// GetStatsDataInDay 获得天统计数据
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) GetStatsDataInDay() *StatsSnapshot {
	return statsItem.computeStatsData(statsItem.CSListDay)
}

// Init 统计单元初始化
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) Init() {

	// 每隔10s执行一次
	samplingInSecondsTicker := system.NewTicker(false, 0, 10*time.Second, func() { statsItem.SamplingInSeconds() })
	samplingInSecondsTicker.Start()

	// 每隔10分钟执行一次
	samplingInMinutesTicker := system.NewTicker(false, 0, 10*time.Minute, func() { statsItem.SamplingInMinutes() })
	samplingInMinutesTicker.Start()

	// 每隔1小时执行一次
	samplingInHourTicker := system.NewTicker(false, 0, 1*time.Hour, func() { statsItem.SamplingInHour() })
	samplingInHourTicker.Start()

	// 分钟整点执行
	diffMin := float64(system.ComputNextMinutesTimeMillis() - system.CurrentTimeMillis())
	var delayMin int = int(math.Abs(diffMin))
	printAtMinutesTicker := system.NewTicker(false, time.Duration(delayMin)*time.Millisecond, time.Minute, func() { statsItem.PrintAtMinutes() })
	printAtMinutesTicker.Start()

	// 小时整点执行
	diffHour := float64(system.ComputNextHourTimeMillis() - system.CurrentTimeMillis())
	var delayHour int = int(math.Abs(diffHour))
	printAtHourTicker := system.NewTicker(false, time.Duration(delayHour)*time.Millisecond, time.Hour, func() { statsItem.PrintAtHour() })
	printAtHourTicker.Start()

	// 每天0点执行
	diffDay := float64(system.ComputNextHourTimeMillis()-system.CurrentTimeMillis()) - 2000
	var delayDay int = int(math.Abs(diffDay))
	printAtDayTicker := system.NewTicker(false, time.Duration(delayDay)*time.Millisecond, 24*time.Hour, func() { statsItem.PrintAtDay() })
	printAtDayTicker.Start()
}

// PrintAtMinutes 输出分钟统计
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) PrintAtMinutes() {
	ss := statsItem.computeStatsData(statsItem.CSListMinute)
	logger.Infof("[%s] [%s] Stats In One Minute, SUM: %d TPS: %.2f AVGPT: %.2f",
		statsItem.StatsName, statsItem.StatsKey, ss.Sum, ss.Tps, ss.Avgpt)
}

// PrintAtHour 输出小时统计
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) PrintAtHour() {
	ss := statsItem.computeStatsData(statsItem.CSListHour)
	logger.Infof("[%s] [%s] Stats In One Hour, SUM: %d TPS: %.2f AVGPT: %.2f",
		statsItem.StatsName, statsItem.StatsKey, ss.Sum, ss.Tps, ss.Avgpt)
}

// PrintAtDay 输出天统计
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) PrintAtDay() {
	ss := statsItem.computeStatsData(statsItem.CSListDay)
	logger.Infof("[%s] [%s] Stats In One Day, SUM: %d TPS: %.2f AVGPT: %.2f",
		statsItem.StatsName, statsItem.StatsKey, ss.Sum, ss.Tps, ss.Avgpt)
}

// SamplingInSeconds 秒统计单元
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) SamplingInSeconds() {

	callSnapshot := callSnapshot{
		timestamp: system.CurrentTimeMillis(),
		times:     atomic.LoadInt64(&(statsItem.TimesCounter)),
		value:     atomic.LoadInt64(&(statsItem.ValueCounter)),
	}
	statsItem.CSListMinute.Put(callSnapshot)
	if statsItem.CSListMinute.Size() > 7 {
		statsItem.CSListMinute.Remove(0)
	}
}

// SamplingInMinutes 分钟统计单元
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) SamplingInMinutes() {

	callSnapshot := callSnapshot{
		timestamp: system.CurrentTimeMillis(),
		times:     atomic.LoadInt64(&(statsItem.TimesCounter)),
		value:     atomic.LoadInt64(&(statsItem.ValueCounter)),
	}

	statsItem.CSListHour.Put(callSnapshot)
	if statsItem.CSListHour.Size() > 7 {
		statsItem.CSListHour.Remove(0)
	}
}

// SamplingInHour 小时统计单元
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) SamplingInHour() {

	callSnapshot := callSnapshot{
		timestamp: system.CurrentTimeMillis(),
		times:     atomic.LoadInt64(&(statsItem.TimesCounter)),
		value:     atomic.LoadInt64(&(statsItem.ValueCounter)),
	}

	statsItem.CSListDay.Put(callSnapshot)
	if statsItem.CSListDay.Size() > 25 {
		statsItem.CSListDay.Remove(0)
	}
}

// StatsItemSet 统计单元集合
// Author rongzhihong
// Since 2017/9/19
type StatsItemSet struct {
	StatsName      string
	statsItemTable map[string]*StatsItem // key: statsKey, val:StatsItem
	allTickers     *system.Tickers       // broker统计的定时任务
	sync.RWMutex
}

// NewStatsItemSet 初始化某个统计维度的统计单元集合
// Author rongzhihong
// Since 2017/9/19
func NewStatsItemSet(statsName string) *StatsItemSet {
	statsItemSet := new(StatsItemSet)
	statsItemSet.StatsName = statsName
	statsItemSet.statsItemTable = make(map[string]*StatsItem, 128)
	statsItemSet.allTickers = system.NewTickers()
	statsItemSet.Init()
	return statsItemSet
}

// GetAndCreateStatsItem 创建、获得statsKey的统计单元
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) GetAndCreateStatsItem(statsKey string) *StatsItem {
	stats.Lock()
	defer stats.Unlock()

	statsItem, ok := stats.statsItemTable[statsKey]
	if !ok || nil == statsItem {
		statsItem = NewStatsItem()
		statsItem.StatsName = stats.StatsName
		statsItem.StatsKey = statsKey
		stats.statsItemTable[statsKey] = statsItem
	}
	return statsItem
}

// AddValue statsKey的统计单元数量增加
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) AddValue(statsKey string, incValue, incTimes int64) {
	statsItem := stats.GetAndCreateStatsItem(statsKey)
	atomic.AddInt64(&(statsItem.ValueCounter), incValue)
	atomic.AddInt64(&(statsItem.TimesCounter), incTimes)
}

// GetStatsDataInMinute 获得statsKey每分钟统计数据
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) GetStatsDataInMinute(statsKey string) *StatsSnapshot {
	stats.RLock()
	defer stats.RUnlock()

	statsItem, ok := stats.statsItemTable[statsKey]
	if ok && statsItem != nil {
		return statsItem.GetStatsDataInMinute()
	}
	return NewStatsSnapshot()
}

// GetStatsDataInHour 获得statsKey每小时统计数据
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) GetStatsDataInHour(statsKey string) *StatsSnapshot {
	stats.RLock()
	defer stats.RUnlock()

	statsItem := stats.statsItemTable[statsKey]
	if statsItem != nil {
		return statsItem.GetStatsDataInHour()
	}
	return NewStatsSnapshot()
}

// GetStatsDataInDay 获得statsKey每天统计数据
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) GetStatsDataInDay(statsKey string) *StatsSnapshot {
	stats.RLock()
	defer stats.RUnlock()

	statsItem := stats.statsItemTable[statsKey]
	if statsItem != nil {
		return statsItem.GetStatsDataInDay()
	}
	return NewStatsSnapshot()
}

// NewStatsItemSet 获得statsKey的统计单元
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) GetStatsItem(statsKey string) *StatsItem {
	stats.RLock()
	defer stats.RUnlock()

	statsItem := stats.statsItemTable[statsKey]
	return statsItem
}

// Init 统计单元集合初始化
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) Init() {
	stats.allTickers.Register("statsItemSet_samplingInSecondsTicker", system.NewTicker(false, 0, 10*time.Second,
		func() { stats.samplingInSeconds() }))

	stats.allTickers.Register("statsItemSet_samplingInMinutesTicker", system.NewTicker(false, 0, 10*time.Minute,
		func() { stats.samplingInMinutes() }))

	stats.allTickers.Register("statsItemSet_samplingInHourTicker", system.NewTicker(false, 0, time.Hour,
		func() { stats.samplingInHour() }))

	diffMin := float64(system.ComputNextMinutesTimeMillis() - system.CurrentTimeMillis())
	var delayMin int = int(math.Abs(diffMin))
	stats.allTickers.Register("statsItemSet_printAtMinutesTicker", system.NewTicker(false, time.Duration(delayMin)*time.Millisecond,
		time.Minute, func() { stats.printAtMinutes() }))

	diffHour := float64(system.ComputNextHourTimeMillis() - system.CurrentTimeMillis())
	var delayHour int = int(math.Abs(diffHour))
	stats.allTickers.Register("statsItemSet_printAtHourTicker", system.NewTicker(false, time.Duration(delayHour)*time.Millisecond,
		time.Hour, func() { stats.printAtHour() }))

	diffDay := float64(system.ComputNextMorningTimeMillis() - system.CurrentTimeMillis())
	var delayDay int = int(math.Abs(diffDay))
	stats.allTickers.Register("statsItemSet_printAtDayTicker", system.NewTicker(false, time.Duration(delayDay)*time.Millisecond,
		24*time.Hour, func() { stats.printAtDay() }))
}

// samplingInSeconds 每秒统计取样
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) samplingInSeconds() {
	stats.RLock()
	defer stats.RUnlock()

	for _, item := range stats.statsItemTable {
		item.SamplingInSeconds()
	}
}

// samplingInMinutes 每分钟统计取样
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) samplingInMinutes() {
	stats.RLock()
	defer stats.RUnlock()

	for _, item := range stats.statsItemTable {
		item.SamplingInMinutes()
	}
}

// samplingInHour 每小时统计取样
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) samplingInHour() {
	stats.RLock()
	defer stats.RUnlock()

	for _, item := range stats.statsItemTable {
		item.SamplingInHour()
	}
}

// printAtMinutes 输出每分钟的统计数据
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) printAtMinutes() {
	stats.RLock()
	defer stats.RUnlock()

	for _, item := range stats.statsItemTable {
		item.PrintAtMinutes()
	}
}

// printAtHour 输出每小时的统计数据
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) printAtHour() {
	stats.RLock()
	defer stats.RUnlock()

	for _, item := range stats.statsItemTable {
		item.PrintAtHour()
	}
}

// printAtHour 输出每天的统计数据
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) printAtDay() {
	stats.RLock()
	defer stats.RUnlock()

	for _, item := range stats.statsItemTable {
		item.PrintAtDay()
	}
}

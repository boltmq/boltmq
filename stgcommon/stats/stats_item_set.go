package stats

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// StatsItemSet 统计单元集合
// Author rongzhihong
// Since 2017/9/19
type StatsItemSet struct {
	sync.RWMutex
	StatsName        string
	StatsItemTable   map[string]*StatsItem // key: statsKey, val:StatsItem
	StatsItemTickers *timeutil.Tickers     // broker统计的定时任务
}

// NewStatsItemSet 初始化某个统计维度的统计单元集合
// Author rongzhihong
// Since 2017/9/19
func NewStatsItemSet(statsName string) *StatsItemSet {
	statsItemSet := new(StatsItemSet)
	statsItemSet.StatsName = statsName
	statsItemSet.StatsItemTable = make(map[string]*StatsItem, 128)
	statsItemSet.StatsItemTickers = timeutil.NewTickers()
	statsItemSet.Init()
	return statsItemSet
}

// GetAndCreateStatsItem 创建、获得statsKey的统计单元
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) GetAndCreateStatsItem(statsKey string) *StatsItem {
	stats.Lock()
	defer stats.Unlock()

	statsItem, ok := stats.StatsItemTable[statsKey]
	if !ok || nil == statsItem {
		statsItem = NewStatsItem()
		statsItem.StatsName = stats.StatsName
		statsItem.StatsKey = statsKey
		stats.StatsItemTable[statsKey] = statsItem
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

	statsItem, ok := stats.StatsItemTable[statsKey]
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

	statsItem := stats.StatsItemTable[statsKey]
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

	statsItem := stats.StatsItemTable[statsKey]
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

	statsItem := stats.StatsItemTable[statsKey]
	return statsItem
}

// Init 统计单元集合初始化
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) Init() {
	stats.StatsItemTickers.Register("statsItemSet_samplingInSecondsTicker", timeutil.NewTicker(false, 0, 10*time.Second,
		func() { stats.samplingInSeconds() }))

	stats.StatsItemTickers.Register("statsItemSet_samplingInMinutesTicker", timeutil.NewTicker(false, 0, 10*time.Minute,
		func() { stats.samplingInMinutes() }))

	stats.StatsItemTickers.Register("statsItemSet_samplingInHourTicker", timeutil.NewTicker(false, 0, time.Hour,
		func() { stats.samplingInHour() }))

	diffMin := float64(stgcommon.ComputNextMinutesTimeMillis() - timeutil.CurrentTimeMillis())
	var delayMin int = int(math.Abs(diffMin))
	stats.StatsItemTickers.Register("statsItemSet_printAtMinutesTicker", timeutil.NewTicker(false, time.Duration(delayMin)*time.Millisecond,
		time.Minute, func() { stats.printAtMinutes() }))

	diffHour := float64(stgcommon.ComputNextHourTimeMillis() - timeutil.CurrentTimeMillis())
	var delayHour int = int(math.Abs(diffHour))
	stats.StatsItemTickers.Register("statsItemSet_printAtHourTicker", timeutil.NewTicker(false, time.Duration(delayHour)*time.Millisecond,
		time.Hour, func() { stats.printAtHour() }))

	diffDay := float64(stgcommon.ComputNextMorningTimeMillis() - timeutil.CurrentTimeMillis())
	var delayDay int = int(math.Abs(diffDay))
	stats.StatsItemTickers.Register("statsItemSet_printAtDayTicker", timeutil.NewTicker(false, time.Duration(delayDay)*time.Millisecond,
		24*time.Hour, func() { stats.printAtDay() }))
}

// samplingInSeconds 每秒统计取样
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) samplingInSeconds() {
	stats.RLock()
	defer stats.RUnlock()

	for _, item := range stats.StatsItemTable {
		item.SamplingInSeconds()
	}
}

// samplingInMinutes 每分钟统计取样
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) samplingInMinutes() {
	stats.RLock()
	defer stats.RUnlock()

	for _, item := range stats.StatsItemTable {
		item.SamplingInMinutes()
	}
}

// samplingInHour 每小时统计取样
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) samplingInHour() {
	stats.RLock()
	defer stats.RUnlock()

	for _, item := range stats.StatsItemTable {
		item.SamplingInHour()
	}
}

// printAtMinutes 输出每分钟的统计数据
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) printAtMinutes() {
	stats.RLock()
	defer stats.RUnlock()

	for _, item := range stats.StatsItemTable {
		item.PrintAtMinutes()
	}
}

// printAtHour 输出每小时的统计数据
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) printAtHour() {
	stats.RLock()
	defer stats.RUnlock()

	for _, item := range stats.StatsItemTable {
		item.PrintAtHour()
	}
}

// printAtHour 输出每天的统计数据
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) printAtDay() {
	stats.RLock()
	defer stats.RUnlock()

	for _, item := range stats.StatsItemTable {
		item.PrintAtDay()
	}
}

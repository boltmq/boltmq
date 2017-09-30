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
	StatsItemTable map[string]*StatsItem // key: statsKey, val:StatsItem
	sync.RWMutex
	StatsName string
}

// NewStatsItemSet 统计单元集合初始化
// Author rongzhihong
// Since 2017/9/19
func NewStatsItemSet(statsName string) *StatsItemSet {
	statsItemSet := new(StatsItemSet)
	statsItemSet.StatsItemTable = make(map[string]*StatsItem)
	statsItemSet.StatsName = statsName
	statsItemSet.Init()
	return statsItemSet
}

// GetAndCreateStatsItem 获得或创建统计单元
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

// AddValue 添加值
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) AddValue(statsKey string, incValue, incTimes int64) {
	statsItem := stats.GetAndCreateStatsItem(statsKey)
	atomic.AddInt64(&(statsItem.ValueCounter), incValue)
	atomic.AddInt64(&(statsItem.TimesCounter), incTimes)
}

// GetStatsDataInMinute 获得分钟统计快照
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) GetStatsDataInMinute(statsKey string) *StatsSnapshot {
	stats.RLock()
	defer stats.RUnlock()

	statsItem := stats.StatsItemTable[statsKey]
	if statsItem != nil {
		return statsItem.GetStatsDataInMinute()
	}
	return NewStatsSnapshot()
}

// GetStatsDataInHour 获得小时统计快照
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

// GetStatsDataInDay 获得天统计快照
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

// NewStatsItemSet 获得统计单元
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
	samplingInSecondsTicker := timeutil.NewTicker(10*1000, 0)
	go samplingInSecondsTicker.Do(func(tm time.Time) {
		stats.samplingInSeconds()
	})

	samplingInMinutesTicker := timeutil.NewTicker(10*60*1000, 0)
	go samplingInMinutesTicker.Do(func(tm time.Time) {
		stats.samplingInMinutes()
	})

	samplingInHourTicker := timeutil.NewTicker(1*60*60*1000, 0)
	go samplingInHourTicker.Do(func(tm time.Time) {
		stats.samplingInHour()
	})

	diffMin := float64(stgcommon.ComputNextMinutesTimeMillis() - timeutil.CurrentTimeMillis())
	var delayMin int = int(math.Abs(diffMin))
	printAtMinutesTicker := timeutil.NewTicker(60000, delayMin)
	go printAtMinutesTicker.Do(func(tm time.Time) {
		stats.printAtMinutes()
	})

	diffHour := float64(stgcommon.ComputNextHourTimeMillis() - timeutil.CurrentTimeMillis())
	var delayHour int = int(math.Abs(diffHour))
	printAtHourTicker := timeutil.NewTicker(3600000, delayHour)
	go printAtHourTicker.Do(func(tm time.Time) {
		stats.printAtHour()
	})

	diffDay := float64(stgcommon.ComputNextHourTimeMillis() - timeutil.CurrentTimeMillis())
	var delayDay int = int(math.Abs(diffDay))
	printAtDayTicker := timeutil.NewTicker(86400000, delayDay)
	go printAtDayTicker.Do(func(tm time.Time) {
		stats.printAtDay()
	})
}

// samplingInSeconds 取样每秒统计
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) samplingInSeconds() {
	stats.RLock()
	defer stats.RUnlock()

	for _, item := range stats.StatsItemTable {
		item.SamplingInSeconds()
	}
}

// samplingInMinutes 取样每分统计
// Author rongzhihong
// Since 2017/9/19
func (stats *StatsItemSet) samplingInMinutes() {
	stats.RLock()
	defer stats.RUnlock()

	for _, item := range stats.StatsItemTable {
		item.SamplingInMinutes()
	}
}

// samplingInHour 取样每小时统计
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

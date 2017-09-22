package stats

import (
	"bytes"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync/list"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"math"
	"sync/atomic"
	"time"
)

// StatsItem 统计单元
// Author rongzhihong
// Since 2017/9/19
type StatsItem struct {
	ValueCounter int64                  `json:"valueCounter"`
	TimesCounter int64                  `json:"timesCounter"`
	CsListMinute *list.BufferLinkedList `json:"csListMinute"`
	CsListHour   *list.BufferLinkedList `json:"csListHour"`
	CsListDay    *list.BufferLinkedList `json:"csListDay"`
	StatsName    string                 `json:"statsName"`
	StatsKey     string                 `json:"statsKey"`
}

// NewStatsItem 统计单元初始化
// Author rongzhihong
// Since 2017/9/19
func NewStatsItem() *StatsItem {
	statsItem := new(StatsItem)
	statsItem.ValueCounter = atomic.AddInt64(&statsItem.ValueCounter, 0)
	statsItem.TimesCounter = atomic.AddInt64(&statsItem.TimesCounter, 0)
	statsItem.CsListMinute = list.NewBufferLinkedList()
	statsItem.CsListHour = list.NewBufferLinkedList()
	statsItem.CsListDay = list.NewBufferLinkedList()
	return statsItem
}

// computeStatsData 计算获得统计数据
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) computeStatsData(csList *list.BufferLinkedList) *StatsSnapshot {
	csList.Lock()
	defer csList.Unlock()
	defer utils.RecoveredFn()

	var (
		tps   float64 = 0.0
		avgpt float64 = 0.0
		sum   int64   = 0
	)

	statsSnapshot := NewStatsSnapshot()
	if csList.Size() > 0 {
		firstBuffer, isHasFirst := csList.Get(0)
		lastBuffer, isHasLast := csList.Get(csList.Size() - 1)

		if isHasFirst && isHasLast && firstBuffer != nil && lastBuffer != nil {
			first := &CallSnapshot{}
			stgcommon.Decode([]byte(firstBuffer.String()), first)

			last := &CallSnapshot{}
			stgcommon.Decode([]byte(lastBuffer.String()), last)

			sum = last.Value - first.Value
			tps = float64(sum) * 1000.0 / float64(last.Timestamp-first.Timestamp)
			timesDiff := last.Times - first.Times
			if timesDiff > 0 {
				avgpt = float64(sum) / float64(timesDiff)
			}
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
	return statsItem.computeStatsData(statsItem.CsListMinute)
}

// GetStatsDataInDay 获得小时统计数据
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) GetStatsDataInHour() *StatsSnapshot {
	return statsItem.computeStatsData(statsItem.CsListHour)
}

// GetStatsDataInDay 获得天统计数据
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) GetStatsDataInDay() *StatsSnapshot {
	return statsItem.computeStatsData(statsItem.CsListDay)
}

// Init 统计单元初始化
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) Init() {
	defer utils.RecoveredFn()

	samplingInSecondsTicker := timeutil.NewTicker(10*1000, 0)
	go samplingInSecondsTicker.Do(func(tm time.Time) {
		statsItem.SamplingInSeconds()
	})

	samplingInMinutesTicker := timeutil.NewTicker(10*60*1000, 0)
	go samplingInMinutesTicker.Do(func(tm time.Time) {
		statsItem.SamplingInMinutes()
	})

	samplingInHourTicker := timeutil.NewTicker(1*60*60*1000, 0)
	go samplingInHourTicker.Do(func(tm time.Time) {
		statsItem.SamplingInHour()
	})

	diffMin := float64(stgcommon.ComputNextMinutesTimeMillis() - timeutil.CurrentTimeMillis())
	var delayMin int = int(math.Abs(diffMin))
	printAtMinutesTicker := timeutil.NewTicker(60000, delayMin)
	go printAtMinutesTicker.Do(func(tm time.Time) {
		statsItem.PrintAtMinutes()
	})

	diffHour := float64(stgcommon.ComputNextHourTimeMillis()-timeutil.CurrentTimeMillis()) - 2000
	var delayHour int = int(math.Abs(diffHour))
	printAtHourTicker := timeutil.NewTicker(3600000, delayHour)
	go printAtHourTicker.Do(func(tm time.Time) {
		statsItem.PrintAtHour()
	})

	diffDay := float64(stgcommon.ComputNextHourTimeMillis() - timeutil.CurrentTimeMillis())
	var delayDay int = int(math.Abs(diffDay))
	printAtDayTicker := timeutil.NewTicker(86400000, delayDay)
	go printAtDayTicker.Do(func(tm time.Time) {
		statsItem.PrintAtDay()
	})
}

// StatsItemSet 输出分钟统计
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) PrintAtMinutes() {
	ss := statsItem.computeStatsData(statsItem.CsListMinute)
	logger.Infof("[%s] [%s] Stats In One Minute, SUM: %d TPS: %.2f AVGPT: %.2f",
		statsItem.StatsName, statsItem.StatsKey, ss.Sum, ss.Tps, ss.Avgpt)
}

// StatsItemSet 输出小时统计
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) PrintAtHour() {
	ss := statsItem.computeStatsData(statsItem.CsListHour)
	logger.Infof("[%s] [%s] Stats In One Hour, SUM: %d TPS: %.2f AVGPT: %.2f",
		statsItem.StatsName, statsItem.StatsKey, ss.Sum, ss.Tps, ss.Avgpt)
}

// StatsItemSet 输出天统计
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) PrintAtDay() {
	ss := statsItem.computeStatsData(statsItem.CsListDay)
	logger.Infof("[%s] [%s] Stats In One Day, SUM: %d TPS: %.2f AVGPT: %.2f",
		statsItem.StatsName, statsItem.StatsKey, ss.Sum, ss.Tps, ss.Avgpt)
}

// StatsItemSet 秒统计单元
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) SamplingInSeconds() {
	statsItem.CsListMinute.Lock()
	defer statsItem.CsListMinute.Unlock()
	defer utils.RecoveredFn()

	callSnapshot := &CallSnapshot{Timestamp: timeutil.CurrentTimeMillis(), Times: statsItem.TimesCounter, Value: statsItem.ValueCounter}
	content := stgcommon.Encode(callSnapshot)
	bytesBuffer := bytes.NewBuffer(content)
	statsItem.CsListMinute.Add(bytesBuffer)

	if statsItem.CsListMinute.Size() > 7 {
		statsItem.CsListMinute.Remove(0)
	}
}

// StatsItemSet 分钟统计单元
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) SamplingInMinutes() {
	statsItem.CsListHour.Lock()
	defer statsItem.CsListHour.Unlock()
	defer utils.RecoveredFn()

	callSnapshot := &CallSnapshot{Timestamp: timeutil.CurrentTimeMillis(), Times: statsItem.TimesCounter, Value: statsItem.ValueCounter}
	content := stgcommon.Encode(callSnapshot)
	bytesBuffer := bytes.NewBuffer(content)
	statsItem.CsListHour.Add(bytesBuffer)

	if statsItem.CsListHour.Size() > 7 {
		statsItem.CsListHour.Remove(0)
	}
}

// StatsItemSet 小时统计单元
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) SamplingInHour() {
	statsItem.CsListDay.Lock()
	defer statsItem.CsListDay.Unlock()
	defer utils.RecoveredFn()

	callSnapshot := &CallSnapshot{Timestamp: timeutil.CurrentTimeMillis(), Times: statsItem.TimesCounter, Value: statsItem.ValueCounter}
	content := stgcommon.Encode(callSnapshot)
	bytesBuffer := bytes.NewBuffer(content)
	statsItem.CsListDay.Add(bytesBuffer)

	if statsItem.CsListDay.Size() > 25 {
		statsItem.CsListDay.Remove(0)
	}
}

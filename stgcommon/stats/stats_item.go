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
	ValueCounter int64                  `json:"valueCounter"` // 具体的统计值
	TimesCounter int64                  `json:"timesCounter"` // 统计次数
	CsListMinute *list.BufferLinkedList `json:"csListMinute"` // 最近一分钟内的镜像，数量6，10秒钟采样一次
	CsListHour   *list.BufferLinkedList `json:"csListHour"`   // 最近一小时内的镜像，数量6，10分钟采样一次
	CsListDay    *list.BufferLinkedList `json:"csListDay"`    // 最近一天内的镜像，数量24，1小时采样一次
	StatsName    string                 `json:"statsName"`
	StatsKey     string                 `json:"statsKey"`
}

// NewStatsItem 统计单元初始化
// Author rongzhihong
// Since 2017/9/19
func NewStatsItem() *StatsItem {
	statsItem := new(StatsItem)
	statsItem.CsListMinute = list.NewBufferLinkedList()
	statsItem.CsListHour = list.NewBufferLinkedList()
	statsItem.CsListDay = list.NewBufferLinkedList()
	atomic.AddInt64(&(statsItem.ValueCounter), 0)
	atomic.AddInt64(&(statsItem.TimesCounter), 0)
	return statsItem
}

// computeStatsData 计算获得统计数据
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) computeStatsData(csList *list.BufferLinkedList) *StatsSnapshot {
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
			stgcommon.Decode(firstBuffer.Bytes(), first)

			last := &CallSnapshot{}
			stgcommon.Decode(lastBuffer.Bytes(), last)

			sum = last.Value - first.Value
			if last.Timestamp-first.Timestamp > 0 {
				tps = float64(sum) * 1000.0 / float64(last.Timestamp-first.Timestamp)
			}

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

	// 每隔10s执行一次
	samplingInSecondsTicker := timeutil.NewTicker(false, 0, 10*time.Second, func() { statsItem.SamplingInSeconds() })
	samplingInSecondsTicker.Start()

	// 每隔10分钟执行一次
	samplingInMinutesTicker := timeutil.NewTicker(false, 0, 10*time.Minute, func() { statsItem.SamplingInMinutes() })
	samplingInMinutesTicker.Start()

	// 每隔1小时执行一次
	samplingInHourTicker := timeutil.NewTicker(false, 0, 1*time.Hour, func() { statsItem.SamplingInHour() })
	samplingInHourTicker.Start()

	// 分钟整点执行
	diffMin := float64(stgcommon.ComputNextMinutesTimeMillis() - timeutil.CurrentTimeMillis())
	var delayMin int = int(math.Abs(diffMin))
	printAtMinutesTicker := timeutil.NewTicker(false, time.Duration(delayMin)*time.Millisecond, time.Minute, func() { statsItem.PrintAtMinutes() })
	printAtMinutesTicker.Start()

	// 小时整点执行
	diffHour := float64(stgcommon.ComputNextHourTimeMillis() - timeutil.CurrentTimeMillis())
	var delayHour int = int(math.Abs(diffHour))
	printAtHourTicker := timeutil.NewTicker(false, time.Duration(delayHour)*time.Millisecond, time.Hour, func() { statsItem.PrintAtHour() })
	printAtHourTicker.Start()

	// 每天0点执行
	diffDay := float64(stgcommon.ComputNextHourTimeMillis()-timeutil.CurrentTimeMillis()) - 2000
	var delayDay int = int(math.Abs(diffDay))
	printAtDayTicker := timeutil.NewTicker(false, time.Duration(delayDay)*time.Millisecond, 24*time.Hour, func() { statsItem.PrintAtDay() })
	printAtDayTicker.Start()
}

// PrintAtMinutes 输出分钟统计
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) PrintAtMinutes() {
	ss := statsItem.computeStatsData(statsItem.CsListMinute)
	logger.Infof("[%s] [%s] Stats In One Minute, SUM: %d TPS: %.2f AVGPT: %.2f",
		statsItem.StatsName, statsItem.StatsKey, ss.Sum, ss.Tps, ss.Avgpt)
}

// PrintAtHour 输出小时统计
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) PrintAtHour() {
	ss := statsItem.computeStatsData(statsItem.CsListHour)
	logger.Infof("[%s] [%s] Stats In One Hour, SUM: %d TPS: %.2f AVGPT: %.2f",
		statsItem.StatsName, statsItem.StatsKey, ss.Sum, ss.Tps, ss.Avgpt)
}

// PrintAtDay 输出天统计
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) PrintAtDay() {
	ss := statsItem.computeStatsData(statsItem.CsListDay)
	logger.Infof("[%s] [%s] Stats In One Day, SUM: %d TPS: %.2f AVGPT: %.2f",
		statsItem.StatsName, statsItem.StatsKey, ss.Sum, ss.Tps, ss.Avgpt)
}

// SamplingInSeconds 秒统计单元
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) SamplingInSeconds() {
	defer utils.RecoveredFn()

	callSnapshot := &CallSnapshot{
		Timestamp: timeutil.CurrentTimeMillis(),
		Times:     atomic.LoadInt64(&(statsItem.TimesCounter)),
		Value:     atomic.LoadInt64(&(statsItem.ValueCounter)),
	}

	content := stgcommon.Encode(callSnapshot)
	statsItem.CsListMinute.Add(bytes.NewBuffer(content))

	if statsItem.CsListMinute.Size() > 7 {
		statsItem.CsListMinute.Remove(0)
	}
}

// SamplingInMinutes 分钟统计单元
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) SamplingInMinutes() {
	defer utils.RecoveredFn()

	callSnapshot := &CallSnapshot{
		Timestamp: timeutil.CurrentTimeMillis(),
		Times:     atomic.LoadInt64(&(statsItem.TimesCounter)),
		Value:     atomic.LoadInt64(&(statsItem.ValueCounter)),
	}

	content := stgcommon.Encode(callSnapshot)
	bytesBuffer := bytes.NewBuffer(content)
	statsItem.CsListHour.Add(bytesBuffer)

	if statsItem.CsListHour.Size() > 7 {
		statsItem.CsListHour.Remove(0)
	}
}

// SamplingInHour 小时统计单元
// Author rongzhihong
// Since 2017/9/19
func (statsItem *StatsItem) SamplingInHour() {
	defer utils.RecoveredFn()

	callSnapshot := &CallSnapshot{
		Timestamp: timeutil.CurrentTimeMillis(),
		Times:     atomic.LoadInt64(&(statsItem.TimesCounter)),
		Value:     atomic.LoadInt64(&(statsItem.ValueCounter)),
	}

	content := stgcommon.Encode(callSnapshot)
	bytesBuffer := bytes.NewBuffer(content)
	statsItem.CsListDay.Add(bytesBuffer)

	if statsItem.CsListDay.Size() > 25 {
		statsItem.CsListDay.Remove(0)
	}
}

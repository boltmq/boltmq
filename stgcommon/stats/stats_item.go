package stats

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync/list"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"math"
	"sync/atomic"
	"time"
)

type StatsItem struct {
	ValueCounter int64 `json:"valueCounter"`
	TimesCounter int64 `json:"timesCounter"`
	// TODO
	CsListMinute *list.BufferLinkedList `json:"csListMinute"`
	CsListHour   *list.BufferLinkedList `json:"csListHour"`
	CsListDay    *list.BufferLinkedList `json:"csListDay"`
	StatsName    string                 `json:"statsName"`
	StatsKey     string                 `json:"statsKey"`
}

func NewStatsItem() *StatsItem {
	statsItem := new(StatsItem)
	statsItem.ValueCounter = atomic.AddInt64(&statsItem.ValueCounter, 0)
	statsItem.TimesCounter = atomic.AddInt64(&statsItem.TimesCounter, 0)
	statsItem.CsListMinute = list.NewBufferLinkedList()
	statsItem.CsListHour = list.NewBufferLinkedList()
	statsItem.CsListDay = list.NewBufferLinkedList()
	return statsItem
}

func (statsItem *StatsItem) computeStatsData(csList *list.BufferLinkedList) *StatsSnapshot {
	statsSnapshot := &StatsSnapshot{}
	csList.Lock()
	defer csList.Unlock()
	var (
		tps   float64
		avgpt float64
		sum   int64
	)
	if csList.Size() > 0 {
		// TODO
	}
	statsSnapshot.Sum = sum
	statsSnapshot.Tps = tps
	statsSnapshot.Avgpt = avgpt
	return statsSnapshot
}

func (statsItem *StatsItem) GetStatsDataInMinute() *StatsSnapshot {
	return statsItem.computeStatsData(statsItem.CsListMinute)
}

func (statsItem *StatsItem) GetStatsDataInHour() *StatsSnapshot {
	return statsItem.computeStatsData(statsItem.CsListHour)
}

func (statsItem *StatsItem) GetStatsDataInDay() *StatsSnapshot {
	return statsItem.computeStatsData(statsItem.CsListDay)
}

func (statsItem *StatsItem) Init() {
	samplingInSecondsTicker := timeutil.NewTicker(10*1000, 0)
	samplingInSecondsTicker.Do(func(tm time.Time) {
		statsItem.SamplingInSeconds()
	})

	samplingInMinutesTicker := timeutil.NewTicker(10*60*1000, 0)
	samplingInMinutesTicker.Do(func(tm time.Time) {
		statsItem.SamplingInMinutes()
	})

	samplingInHourTicker := timeutil.NewTicker(1*60*60*1000, 0)
	samplingInHourTicker.Do(func(tm time.Time) {
		statsItem.SamplingInHour()
	})

	diffMin := float64(stgcommon.ComputNextMinutesTimeMillis() - timeutil.CurrentTimeMillis())
	var delayMin int = int(math.Abs(diffMin))
	printAtMinutesTicker := timeutil.NewTicker(60000, delayMin)
	printAtMinutesTicker.Do(func(tm time.Time) {
		statsItem.PrintAtMinutes()
	})

	diffHour := float64(stgcommon.ComputNextHourTimeMillis() - timeutil.CurrentTimeMillis())
	var delayHour int = int(math.Abs(diffHour))
	printAtHourTicker := timeutil.NewTicker(60000, delayHour)
	printAtHourTicker.Do(func(tm time.Time) {
		statsItem.PrintAtHour()
	})

	diffDay := float64(stgcommon.ComputNextHourTimeMillis() - timeutil.CurrentTimeMillis())
	var delayDay int = int(math.Abs(diffDay))
	printAtDayTicker := timeutil.NewTicker(60000, delayDay)
	printAtDayTicker.Do(func(tm time.Time) {
		statsItem.PrintAtDay()
	})
}

func (statsItem *StatsItem) PrintAtMinutes() {
	ss := statsItem.computeStatsData(statsItem.CsListMinute)
	logger.Infof("[%s] [%s] Stats In One Minute, SUM: %d TPS: %.2f AVGPT: %.2f",
		statsItem.StatsName, statsItem.StatsKey, ss.Sum, ss.Tps, ss.Avgpt)
}

func (statsItem *StatsItem) PrintAtHour() {
	ss := statsItem.computeStatsData(statsItem.CsListHour)
	logger.Infof("[%s] [%s] Stats In One Hour, SUM: %d TPS: %.2f AVGPT: %.2f",
		statsItem.StatsName, statsItem.StatsKey, ss.Sum, ss.Tps, ss.Avgpt)
}

func (statsItem *StatsItem) PrintAtDay() {
	ss := statsItem.computeStatsData(statsItem.CsListDay)
	logger.Infof("[%s] [%s] Stats In One Day, SUM: %d TPS: %.2f AVGPT: %.2f",
		statsItem.StatsName, statsItem.StatsKey, ss.Sum, ss.Tps, ss.Avgpt)
}

func (statsItem *StatsItem) SamplingInSeconds() {
	statsItem.CsListMinute.Lock()
	defer statsItem.CsListMinute.Unlock()

	callSnapshot := &CallSnapshot{Timestamp: timeutil.CurrentTimeMillis(), Times: statsItem.TimesCounter, Value: statsItem.ValueCounter}
	// TODO statsItem.CsListMinute.Add(callSnapshot)
	fmt.Println(callSnapshot)
	if statsItem.CsListMinute.Size() > 7 {
		statsItem.CsListMinute.Remove(0)
	}
}

func (statsItem *StatsItem) SamplingInMinutes() {
	statsItem.CsListHour.Lock()
	defer statsItem.CsListHour.Unlock()

	callSnapshot := &CallSnapshot{Timestamp: timeutil.CurrentTimeMillis(), Times: statsItem.TimesCounter, Value: statsItem.ValueCounter}
	// TODO statsItem.CsListMinute.Add(callSnapshot)
	fmt.Println(callSnapshot)
	if statsItem.CsListHour.Size() > 7 {
		statsItem.CsListHour.Remove(0)
	}
}

func (statsItem *StatsItem) SamplingInHour() {
	statsItem.CsListDay.Lock()
	defer statsItem.CsListDay.Unlock()

	callSnapshot := &CallSnapshot{Timestamp: timeutil.CurrentTimeMillis(), Times: statsItem.TimesCounter, Value: statsItem.ValueCounter}
	// TODO statsItem.CsListMinute.Add(callSnapshot)
	fmt.Println(callSnapshot)
	if statsItem.CsListDay.Size() > 7 {
		statsItem.CsListDay.Remove(0)
	}
}

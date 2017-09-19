package stats

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"math"
	"sync/atomic"
	"time"
)

// MomentStatsItem  MomentStatsItem
// Author rongzhihong
// Since 2017/9/19
type MomentStatsItem struct {
	ValueCounter int64
	StatsName    string
	StatsKey     string
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
	var diff float64 = float64(stgcommon.ComputNextMinutesTimeMillis() - timeutil.CurrentTimeMillis())
	var delay int = int(math.Abs(diff))
	printAtMinutesTicker := timeutil.NewTicker(300000, delay)
	printAtMinutesTicker.Do(func(tm time.Time) {
		item.printAtMinutes()
	})
}

// printAtMinutes  打印
// Author rongzhihong
// Since 2017/9/19
func (item *MomentStatsItem) printAtMinutes() {
	logger.Infof("[%s] [%s] Stats Every 5 Minutes, Value: %d", item.StatsName, item.StatsKey, item.ValueCounter)
}

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
	var diff float64 = float64(stgcommon.ComputNextMinutesTimeMillis() - timeutil.CurrentTimeMillis())
	var delay int = int(math.Abs(diff))
	printAtMinutesTicker := timeutil.NewTicker(false, time.Duration(delay)*time.Millisecond, 5*time.Minute,
		func() {
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

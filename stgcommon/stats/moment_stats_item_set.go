package stats

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// MomentStatsItemSet  MomentStatsItemSet
// Author rongzhihong
// Since 2017/9/17
type MomentStatsItemSet struct {
	StatsItemTable      map[string]*MomentStatsItem `json:"statsItemTable"`
	StatsItemLock       sync.RWMutex                `json:"-"`
	StatsName           string                      `json:"statsName"`
	MomentStatsTaskList []*timeutil.Ticker          // broker统计的定时任务
}

// NewMomentStatsItemSet 初始化统计
// Author gaoyanlei
// Since 2017/8/18
func NewMomentStatsItemSet(statsName string) *MomentStatsItemSet {
	item := new(MomentStatsItemSet)
	item.StatsItemTable = make(map[string]*MomentStatsItem, 128)
	item.StatsName = statsName
	item.init()
	return item
}

// SetValue  SetValue
// Author rongzhihong
// Since 2017/9/19
func (mom *MomentStatsItemSet) SetValue(statsKey string, value int64) {
	statsItem := mom.GetAndCreateStatsItem(statsKey)
	statsItem.ValueCounter = atomic.AddInt64(&value, 0)
}

// GetAndCreateStatsItem  GetAndCreateStatsItem
// Author rongzhihong
// Since 2017/9/19
func (mom *MomentStatsItemSet) GetAndCreateStatsItem(statsKey string) *MomentStatsItem {
	mom.StatsItemLock.Lock()
	defer mom.StatsItemLock.Unlock()
	defer utils.RecoveredFn()

	statsItem := mom.StatsItemTable[statsKey]
	if nil == statsItem {
		statsItem = NewMomentStatsItem()
		statsItem.StatsName = mom.StatsName
		statsItem.StatsKey = statsKey
		mom.StatsItemTable[statsKey] = statsItem
	}
	return statsItem
}

// init  init
// Author rongzhihong
// Since 2017/9/19
func (mom *MomentStatsItemSet) init() {
	diffMin := float64(stgcommon.ComputNextMinutesTimeMillis() - timeutil.CurrentTimeMillis())
	var delayMin int = int(math.Abs(diffMin))
	printAtMinutesTicker := timeutil.NewTicker(300000, delayMin)
	mom.MomentStatsTaskList = append(mom.MomentStatsTaskList, printAtMinutesTicker)
	go printAtMinutesTicker.Do(func(tm time.Time) {
		mom.printAtMinutes()
	})
}

// printAtMinutes  输出每分钟数据
// Author rongzhihong
// Since 2017/9/19
func (mom *MomentStatsItemSet) printAtMinutes() {
	mom.StatsItemLock.Lock()
	defer mom.StatsItemLock.Unlock()
	defer utils.RecoveredFn()

	for _, momentStatsItem := range mom.StatsItemTable {
		momentStatsItem.printAtMinutes()
	}
}

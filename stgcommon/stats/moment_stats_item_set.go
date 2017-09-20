package stats

import "git.oschina.net/cloudzone/smartgo/stgcommon/sync"

// MomentStatsItemSet  MomentStatsItemSet
// Author rongzhihong
// Since 2017/9/17
type MomentStatsItemSet struct {
	StatsItemTable *sync.Map `json:"statsItemTable"`
	StatsName      string    `json:"statsName"`
}

// NewMomentStatsItemSet 初始化统计
// Author gaoyanlei
// Since 2017/8/18
func NewMomentStatsItemSet(statsName string) *MomentStatsItemSet {
	item := new(MomentStatsItemSet)
	item.StatsItemTable = sync.NewMap()
	item.StatsName = statsName
	return item
}

// GetAndCreateStatsItem  GetAndCreateStatsItem
// Author rongzhihong
// Since 2017/9/19
func (mom *MomentStatsItemSet) GetAndCreateStatsItem(statsKey string) *MomentStatsItem {
	// TODO
	return nil
}

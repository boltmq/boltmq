package stats

import "git.oschina.net/cloudzone/smartgo/stgcommon/sync"

type MomentStatsItemSet struct {
	statsItemTable sync.Map
	statsName      string
}

// NewMomentStatsItemSet 初始化统计
// Author gaoyanlei
// Since 2017/8/18
func NewMomentStatsItemSet(statsName string) *MomentStatsItemSet {
	item := new(MomentStatsItemSet)
	item.statsName = statsName
	return item
}

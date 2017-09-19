package stats

import "git.oschina.net/cloudzone/smartgo/stgcommon/sync"

// StatsItemSet 统计单元集合
// Author rongzhihong
// Since 2017/9/19
type StatsItemSet struct {
	SstatsItemTable *sync.Map // key: statsKey, val:StatsItem
	StatsName       string
}

func NewStatsItemSet() *StatsItemSet {
	statsItemSet := new(StatsItemSet)
	statsItemSet.SstatsItemTable = sync.NewMap()
	return statsItemSet
}

func (statsItem *StatsItemSet) GetStatsItem(statsKey string) *StatsItem {
	// TODO
	return nil
}

func (statsItem *StatsItemSet) AddValue(statsKey string, incValue, incTimes int64) {
	// TODO
}

func (statsItem *StatsItemSet) GetAndCreateStatsItem(statsKey string) *StatsItem {
	// TODO
	return nil
}

func (statsItem *StatsItemSet) GetStatsDataInMinute(statsKey string) *StatsSnapshot {
	// TODO
	return nil
}

package stats

// StatsSnapshot Stats Snapshot
// Author rongzhihong
// Since 2017/9/19
type StatsSnapshot struct {
	Sum   int64   `json:"sum"`
	Tps   float64 `json:"tps"`
	Avgpt float64 `json:"avgpt"`
}

// NewStatsSnapshot 初始化
// Author rongzhihong
// Since 2017/9/19
func NewStatsSnapshot() *StatsSnapshot {
	shot := new(StatsSnapshot)
	return shot
}

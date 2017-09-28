package body

// BrokerStatsItem Broker统计最小数据单元
// Author rongzhihong
// Since 2017/9/19
type BrokerStatsItem struct {
	Sum   int64   `json:"sum"`
	Tps   float64 `json:"tps"`
	Avgpt float64 `json:"avgpt"`
}

package body

// BrokerStatsData Broker统计数据
// Author rongzhihong
// Since 2017/9/19
type BrokerStatsData struct {
	StatsMinute *BrokerStatsItem `json:"statsMinute"`
	StatsHour   *BrokerStatsItem `json:"statsHour"`
	StatsDay    *BrokerStatsItem `json:"statsDay"`
}

// BrokerStatsData Broker统计数据
// Author rongzhihong
// Since 2017/9/19
func NewBrokerStatsData() *BrokerStatsData {
	statsData := new(BrokerStatsData)
	statsData.StatsMinute = new(BrokerStatsItem)
	statsData.StatsHour = new(BrokerStatsItem)
	statsData.StatsDay = new(BrokerStatsItem)
	return statsData
}

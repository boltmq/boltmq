package header

// ViewBrokerStatsDataRequestHeader 查看Broker统计信息的请求头
// Author rongzhihong
// Since 2017/9/19
type ViewBrokerStatsDataRequestHeader struct {
	StatsName string `json:"statsName"`
	StatsKey  string `json:"statsKey"`
}

func (header *ViewBrokerStatsDataRequestHeader) CheckFields() error {
	return nil
}

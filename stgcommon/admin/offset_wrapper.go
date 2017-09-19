package admin

// OffsetWrapper 偏移量封装类
// Author rongzhihong
// Since 2017/9/19
type OffsetWrapper struct {
	BrokerOffset   int64 `json:"brokerOffset"`
	ConsumerOffset int64 `json:"consumerOffset"`
	LastTimestamp  int64 `json:"lastTimestamp"`
}

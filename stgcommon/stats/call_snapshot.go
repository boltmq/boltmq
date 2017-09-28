package stats

// CallSnapshot Call Snapshot
// Author rongzhihong
// Since 2017/9/19
type CallSnapshot struct {
	Timestamp int64 `json:"timestamp"`
	Times     int64 `json:"times"`
	Value     int64 `json:"value"`
}

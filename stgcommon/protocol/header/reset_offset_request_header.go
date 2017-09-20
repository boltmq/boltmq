package header

// ResetOffsetRequestHeader 重置偏移量的请求头
// Author rongzhihong
// Since 2017/9/18
type ResetOffsetRequestHeader struct {
	Topic     string `json:"topic"`
	Group     string `json:"group"`
	Timestamp int64  `json:"timestamp"`
	IsForce   bool   `json:"isForce"`
}

func (req *ResetOffsetRequestHeader) CheckFields() error {
	return nil
}

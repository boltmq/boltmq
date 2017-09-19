package header

// ResetOffsetRequestHeader 重置偏移量的请求头
// Author rongzhihong
// Since 2017/9/18
type ResetOffsetRequestHeader struct {
	Topic     string
	Group     string
	Timestamp int64
	IsForce   bool
}

func (req *ResetOffsetRequestHeader) CheckFields() error {
	return nil
}

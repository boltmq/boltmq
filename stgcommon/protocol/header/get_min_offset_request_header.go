package header

// GetMinOffsetRequestHeader 获得最小偏移量的请求头
// Author rongzhihong
// Since 2017/9/19
type GetMinOffsetRequestHeader struct {
	Topic   string
	QueueId int32
}

func (header *GetMinOffsetRequestHeader) CheckFields() error {
	return nil
}

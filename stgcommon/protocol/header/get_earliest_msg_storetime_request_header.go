package header

// GetEarliestMsgStoretimeRequestHeader 获得早期消息存储时间的请求头
// Author rongzhihong
// Since 2017/9/19
type GetEarliestMsgStoretimeRequestHeader struct {
	Topic   string
	QueueId int32
}

func (header *GetEarliestMsgStoretimeRequestHeader) CheckFields() error {
	return nil
}

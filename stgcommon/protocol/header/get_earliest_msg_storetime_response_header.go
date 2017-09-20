package header

// GetEarliestMsgStoretimeResponseHeader 获得早期消息存储时间的返回头
// Author rongzhihong
// Since 2017/9/19
type GetEarliestMsgStoretimeResponseHeader struct {
	Timestamp int64
}

func (header *GetEarliestMsgStoretimeResponseHeader) CheckFields() error {
	return nil
}

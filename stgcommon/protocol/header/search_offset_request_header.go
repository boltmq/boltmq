package header

// SearchOffsetRequestHeader 查询偏移量的请求头
// Author rongzhihong
// Since 2017/9/19
type SearchOffsetRequestHeader struct {
	Topic     string
	QueueId   int32
	Timestamp int64
}

func (header *SearchOffsetRequestHeader) CheckFields() error {
	return nil
}

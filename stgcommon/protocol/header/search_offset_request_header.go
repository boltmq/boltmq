package header

// SearchOffsetRequestHeader 查询偏移量的请求头
// Author rongzhihong
// Since 2017/9/19
type SearchOffsetRequestHeader struct {
	Topic     string `json:"topic"`
	QueueId   int32  `json:"queueId"`
	Timestamp int64  `json:"timestamp"`
}

func (header *SearchOffsetRequestHeader) CheckFields() error {
	return nil
}

package header

// queryConsumeTimeSpan 根据 topic 和 group 获取消息的时间跨度的请求头
// Author rongzhihong
// Since 2017/9/19
type QueryConsumeTimeSpanRequestHeader struct {
	Topic string `json:"topic"`
	Group string `json:"group"`
}

func (header *QueryConsumeTimeSpanRequestHeader) CheckFields() error {
	return nil
}

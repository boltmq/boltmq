package header

// queryTopicConsumeByWho 查询Topic被哪些消费者消费的请求头
// Author rongzhihong
// Since 2017/9/19
type QueryTopicConsumeByWhoRequestHeader struct {
	Topic string `json:"topic"`
}

func (header *QueryTopicConsumeByWhoRequestHeader) CheckFields() error {
	return nil
}

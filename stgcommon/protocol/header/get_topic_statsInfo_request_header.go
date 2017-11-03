package header

// GetTopicStatsInfoRequestHeader 获得Topic统计信息的请求头
// Author rongzhihong
// Since 2017/9/19
type GetTopicStatsInfoRequestHeader struct {
	Topic string `json:"topic"`
}

func (header *GetTopicStatsInfoRequestHeader) CheckFields() error {
	return nil
}
func NewGetTopicStatsInfoRequestHeader(topic string) *GetTopicStatsInfoRequestHeader {
	topicStatsInfoRequestHeader := &GetTopicStatsInfoRequestHeader{
		Topic: topic,
	}
	return topicStatsInfoRequestHeader
}

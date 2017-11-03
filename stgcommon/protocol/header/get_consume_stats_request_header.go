package header

// GetConsumeStatsRequestHeader 获得消费者统计信息的请求头
// Author rongzhihong
// Since 2017/9/19
type GetConsumeStatsRequestHeader struct {
	Topic         string `json:"topic"`
	ConsumerGroup string `json:"consumerGroup"`
}

func (header *GetConsumeStatsRequestHeader) CheckFields() error {
	return nil
}

func NewGetConsumeStatsRequestHeader(consumerGroup, topic string) *GetConsumeStatsRequestHeader {
	consumeStatsRequestHeader := &GetConsumeStatsRequestHeader{
		Topic:         topic,
		ConsumerGroup: consumerGroup,
	}
	return consumeStatsRequestHeader
}

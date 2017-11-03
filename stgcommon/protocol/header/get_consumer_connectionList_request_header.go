package header

// GetConsumerConnectionListRequestHeader 获得Toipc统计信息的请求头
// Author rongzhihong
// Since 2017/9/19
type GetConsumerConnectionListRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
}

func (header *GetConsumerConnectionListRequestHeader) CheckFields() error {
	return nil
}

func NewGetConsumerConnectionListRequestHeader(consumerGroup string) *GetConsumerConnectionListRequestHeader {
	consumerConnectionListRequestHeader := &GetConsumerConnectionListRequestHeader{
		ConsumerGroup: consumerGroup,
	}
	return consumerConnectionListRequestHeader
}

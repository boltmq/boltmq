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

// GetConsumerConnectionListRequestHeader 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
func NewGetConsumerConnectionListRequestHeader(consumerGroup string) *GetConsumerConnectionListRequestHeader {
	consumerConnectionListRequestHeader := &GetConsumerConnectionListRequestHeader{
		ConsumerGroup: consumerGroup,
	}
	return consumerConnectionListRequestHeader
}

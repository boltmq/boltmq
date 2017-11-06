package header

// GetConsumerRunningInfoRequestHeader 获取Consumer内存数据结构的请求头
// Author rongzhihong
// Since 2017/9/19
type GetConsumerRunningInfoRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	ClientId      string `json:"clientId"`
	JstackEnable  bool   `json:"jstackEnable"`
}

func (header *GetConsumerRunningInfoRequestHeader) CheckFields() error {
	return nil
}

func NewGetConsumerRunningInfoRequestHeader(consumerGroup, clientId string, jstackEnable bool) *GetConsumerRunningInfoRequestHeader {
	consumerRunningInfoRequestHeader := &GetConsumerRunningInfoRequestHeader{
		ConsumerGroup: consumerGroup,
		ClientId:      clientId,
		JstackEnable:  jstackEnable,
	}
	return consumerRunningInfoRequestHeader
}

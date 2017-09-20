package header

// consumeMessageDirectlyResultRequestHeader consumeMessageDirectlyResult请求头
// Author rongzhihong
// Since 2017/9/19
type ConsumeMessageDirectlyResultRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	ClientId      string `json:"clientId"`
	MsgId         string `json:"msgId"`
	BrokerName    string `json:"brokerName"`
}

func (header *ConsumeMessageDirectlyResultRequestHeader) CheckFields() error {
	return nil
}

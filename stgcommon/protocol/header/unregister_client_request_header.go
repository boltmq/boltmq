package header

// UnregisterClientRequestHeader: 注销客户端
// Author: yintongqiang
// Since:  2017/8/17
type UnregisterClientRequestHeader struct {
	ClientID      string `json:"clientID"`
	ProducerGroup string `json:"producerGroup"`
	ConsumerGroup string `json:"consumerGroup"`
}

func (header *UnregisterClientRequestHeader) CheckFields() error {
	return nil
}

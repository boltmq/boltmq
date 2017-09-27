package header

// ConsumerSendMsgBackRequestHeader: 消费消息头
// Author: yintongqiang
// Since:  2017/8/17
type ConsumerSendMsgBackRequestHeader struct {
	Offset      int64  `json:"offset"`
	Group       string `json:"group"`
	DelayLevel  int32  `json:"delayLevel"`
	OriginMsgId string `json:"originMsgId"`
	OriginTopic string `json:"originTopic"`
	UnitMode    bool   `json:"unitMode"`
}

func (header *ConsumerSendMsgBackRequestHeader) CheckFields() error {
	return nil
}

// 初始化 ConsumerSendMsgBackRequestHeader
// Author gaoyanlei
// Since 2017/8/17
func NewConsumerSendMsgBackRequestHeader() *ConsumerSendMsgBackRequestHeader {
	return &ConsumerSendMsgBackRequestHeader{
		UnitMode: false,
	}
}

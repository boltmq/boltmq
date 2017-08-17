package header
// ConsumerSendMsgBackRequestHeader: 消费消息头
// Author: yintongqiang
// Since:  2017/8/17
type ConsumerSendMsgBackRequestHeader struct {
	Offset      int64
	Group       string
	DelayLevel  int
	OriginMsgId string
	OriginTopic string
	UnitMode    bool
}

func (header*ConsumerSendMsgBackRequestHeader)CheckFields() {

}

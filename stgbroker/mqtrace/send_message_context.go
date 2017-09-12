package mqtrace

// SendMessageContext 消息发送上下文
// Author gaoyanlei
// Since 2017/8/15
type SendMessageContext struct {
	ProducerGroup string
	Topic         string
	MsgId         string
	OriginMsgId   string
	QueueId       int32
	QueueOffset   int64
	BrokerAddr    string
	BornHost      string
	BodyLength    int
	Code          int
	ErrorMsg      string
	MsgProps      string
}


// NewSendMessageContext 初始化
// Author gaoyanlei
// Since 2017/8/15
func NewSendMessageContext() *SendMessageContext {
	return &SendMessageContext{
	}
}

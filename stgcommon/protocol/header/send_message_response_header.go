package header

// SendMessageResponseHeader: 发送消息响应头
// Author: yintongqiang
// Since:  2017/8/16
type SendMessageResponseHeader struct {
	MsgId         string
	QueueId       int32
	QueueOffset   int64
	TransactionId string
}

func (header *SendMessageResponseHeader) CheckFields() error {
	return nil
}

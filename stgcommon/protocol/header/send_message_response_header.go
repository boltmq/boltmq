package header

// SendMessageResponseHeader: 发送消息响应头
// Author: yintongqiang
// Since:  2017/8/16
type SendMessageResponseHeader struct {
	MsgId         string `json:"msgId"`
	QueueId       int32  `json:"queueId"`
	QueueOffset   int64  `json:"queueOffset"`
	TransactionId string `json:"transactionId"`
}

func (header *SendMessageResponseHeader) CheckFields() error {
	return nil
}

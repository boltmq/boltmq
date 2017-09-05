package header

// UpdateConsumerOffsetRequestHeader: 更新消费offset的请求头
// Author: yintongqiang
// Since:  2017/8/11
type UpdateConsumerOffsetRequestHeader struct {
	ConsumerGroup string
	Topic         string
	QueueId       int
	CommitOffset  int64
}

func (header *UpdateConsumerOffsetRequestHeader) CheckFields() error {
	return nil
}

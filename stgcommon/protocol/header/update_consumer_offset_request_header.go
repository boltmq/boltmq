package header

// UpdateConsumerOffsetRequestHeader: 更新消费offset的请求头
// Author: yintongqiang
// Since:  2017/8/11
type UpdateConsumerOffsetRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueId       int    `json:"queueId"`
	CommitOffset  int64  `json:"commitOffset"`
}

func (header *UpdateConsumerOffsetRequestHeader) CheckFields() error {
	return nil
}

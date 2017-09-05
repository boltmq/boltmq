package header

// QueryConsumerOffsetRequestHeader: 查询消费offset
// Author: yintongqiang
// Since:  2017/8/24
type QueryConsumerOffsetRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueId       int32  `json:"queueId"`
}

func (header *QueryConsumerOffsetRequestHeader) CheckFields() error {
	return nil
}

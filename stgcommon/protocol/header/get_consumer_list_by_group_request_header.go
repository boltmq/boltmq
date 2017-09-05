package header

// GetConsumerListByGroupRequestHeader: 获取消费列表
// Author: yintongqiang
// Since:  2017/8/11
type GetConsumerListByGroupRequestHeader struct {
	ConsumerGroup string
}

func (header *GetConsumerListByGroupRequestHeader) CheckFields() error {
	return nil
}

package header

// GetMaxOffsetRequestHeader: 获取队列最大offset
// Author: yintongqiang
// Since:  2017/8/23
type GetMaxOffsetRequestHeader struct {
	Topic   string
	QueueId int
}

func (header *GetMaxOffsetRequestHeader) CheckFields() error {
	return nil
}

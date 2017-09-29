package header

// GetMaxOffsetRequestHeader: 获取队列最大offset
// Author: yintongqiang
// Since:  2017/8/23
type GetMaxOffsetRequestHeader struct {
	Topic   string `json:"topic"`
	QueueId int    `json:"queueId"`
}

func NewGetMaxOffsetRequestHeader() *GetMaxOffsetRequestHeader {
	header := new(GetMaxOffsetRequestHeader)
	return header
}

func (header *GetMaxOffsetRequestHeader) CheckFields() error {
	return nil
}

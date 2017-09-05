package header

// QueryConsumerOffsetResponseHeader: 查询offset响应头
// Author: yintongqiang
// Since:  2017/8/24
type QueryConsumerOffsetResponseHeader struct {
	Offset int64 `json:"offset"`
}

func (header *QueryConsumerOffsetResponseHeader) CheckFields() error {
	return nil
}

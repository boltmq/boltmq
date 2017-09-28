package header

// GetMaxOffsetResponseHeader: 最大偏移响应头
// Author: yintongqiang
// Since:  2017/8/23
type GetMaxOffsetResponseHeader struct {
	Offset int64 `json:"offset"`
}

func (header *GetMaxOffsetResponseHeader) CheckFields() error {
	return nil
}

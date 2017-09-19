package header

// GetMinOffsetResponseHeader 获得最小偏移量的返回头
// Author rongzhihong
// Since 2017/9/19
type GetMinOffsetResponseHeader struct {
	Offset int64
}

func (header *GetMinOffsetResponseHeader) CheckFields() error {
	return nil
}

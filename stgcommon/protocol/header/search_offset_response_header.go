package header

// SearchOffsetResponseHeader 查询偏移量的返回头
// Author rongzhihong
// Since 2017/9/19
type SearchOffsetResponseHeader struct {
	Offset int64
}

func (header *SearchOffsetResponseHeader) CheckFields() error {
	return nil
}

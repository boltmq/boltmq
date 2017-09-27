package header

// QueryMessageResponseHeader 查询消息返回头
// Author rongzhihong
// Since 2017/9/18
type QueryMessageResponseHeader struct {
	IndexLastUpdateTimestamp int64 `json:"indexLastUpdateTimestamp"`
	IndexLastUpdatePhyoffset int64 `json:"indexLastUpdatePhyoffset"`
}

func (query QueryMessageResponseHeader) CheckFields() error {
	return nil
}

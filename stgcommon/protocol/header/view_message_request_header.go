package header

// ViewMessageRequestHeader 根据MsgId查询消息的请求头
// Author rongzhihong
// Since 2017/9/18
type ViewMessageRequestHeader struct {
	Offset uint64 `json:"offset"`
}

func (header *ViewMessageRequestHeader) CheckFields() error {
	return nil
}

package header

// QueryCorrectionOffsetRequestHeader 查找被修正 offset (转发组件）的请求头
// Author rongzhihong
// Since 2017/9/19
type QueryCorrectionOffsetRequestHeader struct {
	FilterGroups string `json:"filterGroups"`
	CompareGroup string `json:"compareGroup"`
	Topic        string `json:"topic"`
}

func (header *QueryCorrectionOffsetRequestHeader) CheckFields() error {
	return nil
}

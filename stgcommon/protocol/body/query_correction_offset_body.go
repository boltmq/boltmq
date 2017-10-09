package body

// QueryCorrectionOffsetBody 查找被修正 offset (转发组件）的返回内容
// Author rongzhihong
// Since 2017/9/19
type QueryCorrectionOffsetBody struct {
	CorrectionOffsets map[int]int64 `json:"correctionOffsets"`
}

func NewQueryCorrectionOffsetBody() *QueryCorrectionOffsetBody {
	body := new(QueryCorrectionOffsetBody)
	body.CorrectionOffsets = make(map[int]int64)
	return body
}

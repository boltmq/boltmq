package body

import (
	set "github.com/deckarep/golang-set"
)

// QueryConsumeTimeSpanBody 查询消费时间跨度
// Author rongzhihong
// Since 2017/9/19
type QueryConsumeTimeSpanBody struct {
	ConsumeTimeSpanSet set.Set `json:"consumeTimeSpanSet"`
}

func NewQueryConsumeTimeSpanBody() *QueryConsumeTimeSpanBody {
	body := new(QueryConsumeTimeSpanBody)
	body.ConsumeTimeSpanSet = set.NewSet()
	return body
}

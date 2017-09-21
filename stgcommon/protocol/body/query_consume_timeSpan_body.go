package body

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	set "github.com/deckarep/golang-set"
)

// QueryConsumeTimeSpanBody 查询消费时间跨度
// Author rongzhihong
// Since 2017/9/19
type QueryConsumeTimeSpanBody struct {
	ConsumeTimeSpanSet set.Set `json:"consumeTimeSpanSet"`
	*protocol.RemotingSerializable
}

func NewQueryConsumeTimeSpanBody() *QueryConsumeTimeSpanBody {
	body := new(QueryConsumeTimeSpanBody)
	body.ConsumeTimeSpanSet = set.NewSet()
	body.RemotingSerializable = new(protocol.RemotingSerializable)
	return body
}

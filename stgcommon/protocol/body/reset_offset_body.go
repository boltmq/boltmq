package body

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// ResetOffsetBody 重置偏移量的body
// Author rongzhihong
// Since 2017/9/18
type ResetOffsetBody struct {
	OffsetTable map[*message.MessageQueue]int64
	*protocol.RemotingSerializable
}

func NewResetOffsetBody() *ResetOffsetBody {
	body := new(ResetOffsetBody)
	return body
}

package body

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// ResetOffsetBody 重置偏移量的body
// Author rongzhihong
// Since 2017/9/18
type ResetOffsetBody struct {
	OffsetTable map[*message.MessageQueue]int64 `json:"offsetTable"`
	*protocol.RemotingSerializable
}

func NewResetOffsetBody() *ResetOffsetBody {
	body := new(ResetOffsetBody)
	body.OffsetTable = make(map[*message.MessageQueue]int64)
	body.RemotingSerializable = new(protocol.RemotingSerializable)
	return body
}

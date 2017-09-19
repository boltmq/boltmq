package body

import "git.oschina.net/cloudzone/smartgo/stgnet/protocol"

// QueryCorrectionOffsetBody 查找被修正 offset (转发组件）的返回内容
// Author rongzhihong
// Since 2017/9/19
type QueryCorrectionOffsetBody struct {
	CorrectionOffsets map[int]int64 `json:"correctionOffsets"`
	*protocol.RemotingSerializable
}

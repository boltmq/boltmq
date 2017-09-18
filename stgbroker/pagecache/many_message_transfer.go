package pagecache

import (
	"bytes"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
)

// ManyMessageTransfer  消息转移
// Author rongzhihong
// Since 2017/9/17
type ManyMessageTransfer struct {
	byteBufferHeader *bytes.Buffer
	getMessageResult *stgstorelog.GetMessageResult
	transfered       int64
}

// NewManyMessageTransfer  初始化
// Author rongzhihong
// Since 2017/9/17
func NewManyMessageTransfer(byteBufferHeader *bytes.Buffer, getMessageResult *stgstorelog.GetMessageResult) *ManyMessageTransfer {
	mmt := new(ManyMessageTransfer)
	mmt.byteBufferHeader = byteBufferHeader
	mmt.getMessageResult = getMessageResult
	return mmt
}

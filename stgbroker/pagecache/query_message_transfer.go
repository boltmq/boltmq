package pagecache

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
	"bytes"
)

// OneMessageTransfer  消息转移
// Author rongzhihong
// Since 2017/9/17
type QueryMessageTransfer struct {
	remotingCommand    *protocol.RemotingCommand
	queryMessageResult *stgstorelog.QueryMessageResult
}

// NewOneMessageTransfer 初始化OneMessageTransfer
// Author rongzhihong
// Since 2017/9/17
func NewQueryMessageTransfer(response *protocol.RemotingCommand, queryMessageResult *stgstorelog.QueryMessageResult) *QueryMessageTransfer {
	omt := new(QueryMessageTransfer)
	omt.remotingCommand = response
	omt.queryMessageResult = queryMessageResult
	omt.EncodeBody()
	return omt
}

// EncodeBody 将MessageBufferList存入Body中
// Author rongzhihong
// Since 2017/9/17
func (omt *QueryMessageTransfer) EncodeBody() {
	if omt.queryMessageResult == nil || omt.queryMessageResult.MessageBufferList == nil {
		return
	}

	bodyBuffer := bytes.NewBuffer([]byte{})
	for _, mappedByteBuffer := range omt.queryMessageResult.MessageBufferList {
		bodyBuffer.Write(mappedByteBuffer.Bytes())
	}
	omt.remotingCommand.Body = bodyBuffer.Bytes()
}

// Bytes  实现Serirable接口
// Author rongzhihong
// Since 2017/9/17
func (omt *QueryMessageTransfer) Bytes() []byte {
	return omt.remotingCommand.Bytes()
}

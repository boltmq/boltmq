package pagecache

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
)

// OneMessageTransfer  消息转移
// Author rongzhihong
// Since 2017/9/17
type OneMessageTransfer struct {
	remotingCommand         *protocol.RemotingCommand
	selectMapedBufferResult *stgstorelog.SelectMapedBufferResult
}

// NewOneMessageTransfer 初始化OneMessageTransfer
// Author rongzhihong
// Since 2017/9/17
func NewOneMessageTransfer(response *protocol.RemotingCommand, selectMapedBufferResult *stgstorelog.SelectMapedBufferResult) *OneMessageTransfer {
	nmt := new(OneMessageTransfer)
	nmt.remotingCommand = response
	nmt.selectMapedBufferResult = selectMapedBufferResult
	nmt.EncodeBody()
	return nmt
}

// EncodeBody 将MessageBufferList存入Body中
// Author rongzhihong
// Since 2017/9/17
func (mmt *OneMessageTransfer) EncodeBody() {
	if mmt.selectMapedBufferResult == nil || mmt.selectMapedBufferResult.MappedByteBuffer == nil {
		return
	}
	mmt.remotingCommand.Body = mmt.selectMapedBufferResult.MappedByteBuffer.Bytes()
}

// Bytes  实现Serirable接口
// Author rongzhihong
// Since 2017/9/17
func (mmt *OneMessageTransfer) Bytes() []byte {
	return mmt.remotingCommand.Bytes()
}

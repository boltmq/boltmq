package pagecache

import (
	"bytes"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
)

// ManyMessageTransfer  消息转移
// Author rongzhihong
// Since 2017/9/17
type ManyMessageTransfer struct {
	remotingCommand  *protocol.RemotingCommand
	getMessageResult *stgstorelog.GetMessageResult
}

// NewManyMessageTransfer  初始化
// Author rongzhihong
// Since 2017/9/17
func NewManyMessageTransfer(remotingCommand *protocol.RemotingCommand, getMessageResult *stgstorelog.GetMessageResult) *ManyMessageTransfer {
	mmt := new(ManyMessageTransfer)
	mmt.remotingCommand = remotingCommand
	mmt.getMessageResult = getMessageResult
	mmt.EncodeBody()
	return mmt
}

// EncodeBody 将MessageBufferList存入Body中
// Author rongzhihong
// Since 2017/9/17
func (mmt *ManyMessageTransfer) EncodeBody() {
	if mmt.getMessageResult == nil || mmt.getMessageResult.MessageBufferList.Len() <= 0 {
		return
	}

	bodyBuffer := bytes.NewBuffer([]byte{})
	for e := mmt.getMessageResult.MessageBufferList.Front(); e != nil; e = e.Next() {
		if mapedBufferResult, ok := e.Value.(*stgstorelog.SelectMapedBufferResult); ok {
			bodyBuffer.Write(mapedBufferResult.MappedByteBuffer.Bytes())
		}
	}
	mmt.remotingCommand.Body = bodyBuffer.Bytes()
}

// Bytes  实现Serirable接口
// Author rongzhihong
// Since 2017/9/17
func (mmt *ManyMessageTransfer) Bytes() []byte {
	return mmt.remotingCommand.Bytes()
}

// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package pagecache

import (
	"bytes"

	"github.com/boltmq/boltmq/store"
	"github.com/boltmq/common/protocol"
)

// ManyMessageTransfer  消息转移
// Author rongzhihong
// Since 2017/9/17
type ManyMessageTransfer struct {
	remotingCommand  *protocol.RemotingCommand
	getMessageResult *store.GetMessageResult
}

// NewManyMessageTransfer  初始化
// Author rongzhihong
// Since 2017/9/17
func NewManyMessageTransfer(remotingCommand *protocol.RemotingCommand, getMessageResult *store.GetMessageResult) *ManyMessageTransfer {
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
		if bufferResult, ok := e.Value.(store.ByteBuffer); ok {
			bodyBuffer.Write(bufferResult.Bytes())
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

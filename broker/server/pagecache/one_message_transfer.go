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
	"github.com/boltmq/boltmq/store"
	"github.com/boltmq/common/protocol"
)

// OneMessageTransfer  消息转移
// Author rongzhihong
// Since 2017/9/17
type OneMessageTransfer struct {
	remotingCommand *protocol.RemotingCommand
	bufferResult    store.BufferResult
}

// NewOneMessageTransfer 初始化OneMessageTransfer
// Author rongzhihong
// Since 2017/9/17
func NewOneMessageTransfer(response *protocol.RemotingCommand, bufferResult store.BufferResult) *OneMessageTransfer {
	nmt := new(OneMessageTransfer)
	nmt.remotingCommand = response
	nmt.bufferResult = bufferResult
	nmt.EncodeBody()
	return nmt
}

// EncodeBody 将MessageBufferList存入Body中
// Author rongzhihong
// Since 2017/9/17
func (mmt *OneMessageTransfer) EncodeBody() {
	if mmt.bufferResult == nil || mmt.bufferResult.Buffer() == nil {
		return
	}
	mmt.remotingCommand.Body = mmt.bufferResult.Buffer().Bytes()
}

// Bytes  实现Serirable接口
// Author rongzhihong
// Since 2017/9/17
func (mmt *OneMessageTransfer) Bytes() []byte {
	return mmt.remotingCommand.Bytes()
}

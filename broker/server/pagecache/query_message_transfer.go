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

// OneMessageTransfer  消息转移
// Author rongzhihong
// Since 2017/9/17
type QueryMessageTransfer struct {
	remotingCommand    *protocol.RemotingCommand
	queryMessageResult *store.QueryMessageResult
}

// NewOneMessageTransfer 初始化OneMessageTransfer
// Author rongzhihong
// Since 2017/9/17
func NewQueryMessageTransfer(response *protocol.RemotingCommand, queryMessageResult *store.QueryMessageResult) *QueryMessageTransfer {
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

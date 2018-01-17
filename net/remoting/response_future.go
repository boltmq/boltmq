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
package remoting

import (
	"time"

	"fmt"

	"github.com/boltmq/common/protocol"
)

// ResponseFuture response future
type ResponseFuture struct {
	responseCommand *protocol.RemotingCommand
	sendRequestOK   bool
	err             error
	opaque          int32
	timeoutMillis   int64
	invokeCallback  InvokeCallback
	beginTimestamp  int64
	done            chan bool
}

func newResponseFuture(opaque int32, timeoutMillis int64) *ResponseFuture {
	return &ResponseFuture{
		sendRequestOK:  false,
		opaque:         opaque,
		timeoutMillis:  timeoutMillis,
		beginTimestamp: time.Now().Unix() * 1000,
	}
}

// IsTimeout 是否超时
func (responseFuture *ResponseFuture) IsTimeout() bool {
	currentTimeMillis := time.Now().Unix() * 1000
	return currentTimeMillis-responseFuture.beginTimestamp > responseFuture.timeoutMillis
}

// IsSendRequestOK 是否发送成功
func (responseFuture *ResponseFuture) IsSendRequestOK() bool {
	return responseFuture.sendRequestOK
}

// GetRemotingCommand 获取RemotingCommand
func (responseFuture *ResponseFuture) GetRemotingCommand() *protocol.RemotingCommand {
	return responseFuture.responseCommand
}

// String ResponseFuture结构体字符串
func (r *ResponseFuture) String() string {
	if r == nil {
		return "<nil>"
	}

	var isSYNC bool
	if r.done != nil {
		isSYNC = true
	}

	return fmt.Sprintf("{sendRequestOK=%t, err=%v, opaque=%d, timeoutMillis=%d, beginTimestamp=%d, sync=%t, %s}",
		r.sendRequestOK, r.err, r.opaque, r.timeoutMillis, r.beginTimestamp, isSYNC, r.responseCommand)
}

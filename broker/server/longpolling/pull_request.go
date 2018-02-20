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
package longpolling

import (
	"github.com/boltmq/common/net/core"
	"github.com/boltmq/common/protocol"
)

// PullRequest 拉消息请求
// Author gaoyanlei
// Since 2017/8/18
type PullRequest struct {
	RequestCommand     *protocol.RemotingCommand
	Context            core.Context
	TimeoutMillis      int64
	SuspendTimestamp   int64
	PullFromThisOffset int64
}

func NewPullRequest(requestCommand *protocol.RemotingCommand, ctx core.Context, timeoutMillis, suspendTimestamp, pullFromThisOffset int64) *PullRequest {
	var pullRequest = new(PullRequest)
	pullRequest.TimeoutMillis = timeoutMillis
	pullRequest.SuspendTimestamp = suspendTimestamp
	pullRequest.PullFromThisOffset = pullFromThisOffset
	pullRequest.RequestCommand = requestCommand
	pullRequest.Context = ctx
	return pullRequest
}

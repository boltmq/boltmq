package longpolling

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// PullRequest 拉消息请求
// Author gaoyanlei
// Since 2017/8/18
type PullRequest struct {
	RequestCommand     *protocol.RemotingCommand
	Context            netm.Context
	TimeoutMillis      int64
	SuspendTimestamp   int64
	PullFromThisOffset int64
}

func NewPullRequest(requestCommand *protocol.RemotingCommand, ctx netm.Context, timeoutMillis, suspendTimestamp, pullFromThisOffset int64) *PullRequest {
	var pullRequest = new(PullRequest)
	pullRequest.TimeoutMillis = timeoutMillis
	pullRequest.SuspendTimestamp = suspendTimestamp
	pullRequest.PullFromThisOffset = pullFromThisOffset
	pullRequest.RequestCommand = requestCommand
	pullRequest.Context = ctx
	return pullRequest
}

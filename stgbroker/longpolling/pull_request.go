package longpolling

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// PullRequest 拉消息请求
// Author gaoyanlei
// Since 2017/8/18
type PullRequest struct {
	requestCommand protocol.RemotingCommand
	// TODO Channel clientChannel;
	timeoutMillis      int64
	suspendTimestamp   int64
	pullFromThisOffset int64
}

func NewPullRequest(requestCommand protocol.RemotingCommand, timeoutMillis, suspendTimestamp, pullFromThisOffset int64) *PullRequest {
	var pullRequest = new(PullRequest)
	pullRequest.timeoutMillis = timeoutMillis
	pullRequest.suspendTimestamp = suspendTimestamp
	pullRequest.pullFromThisOffset = pullFromThisOffset
	pullRequest.requestCommand = requestCommand
	return pullRequest
}

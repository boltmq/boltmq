package remoting

import (
	"time"

	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
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

func (responseFuture *ResponseFuture) isTimeout() bool {
	currentTimeMillis := time.Now().Unix() * 1000
	return currentTimeMillis-responseFuture.beginTimestamp > responseFuture.timeoutMillis
}

func (responseFuture *ResponseFuture) isSendRequestOK() bool {
	return responseFuture.sendRequestOK
}

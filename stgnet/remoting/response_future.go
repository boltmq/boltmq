package remoting

import (
	"time"

	"fmt"
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

// String 格式化ResponseFuture结构体
func (r *ResponseFuture) String() string {
	if r == nil {
		return "ResponseFuture is nil"
	}

	val := "invokeCallback is nil"
	if r.invokeCallback != nil {
		val = "invokeCallback is not nil"
	}

	command := r.responseCommand.ToString()
	format := "ResponseFuture {sendRequestOK=%t, err=%v, opaque=%d, timeoutMillis=%d, beginTimestamp=%d, done=%t, %s, %s}"
	return fmt.Sprintf(format, r.sendRequestOK, r.err, r.opaque, r.timeoutMillis, r.beginTimestamp, r.done, val, command)
}

package remoting

import "git.oschina.net/cloudzone/smartgo/stgnet/protocol"

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

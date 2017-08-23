package remoting

import (
	"net"

	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// RemotingServer remoting server define
type RemotingServer interface {
	InvokeSync(conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64) (*protocol.RemotingCommand, error)
	InvokeAsync(conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error
	InvokeOneway(conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64) error
	RegisterProcessor(requestCode int, processor RequestProcessor)
	RegisterDefaultProcessor(processor RequestProcessor)
	RegisterRPCHook(rpcHook RPCHook)
	Start()
	Shutdown()
}

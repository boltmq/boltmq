package remoting

import (
	"github.com/boltmq/boltmq/net/core"
	"github.com/boltmq/common/protocol"
)

// RemotingServer remoting server define
type RemotingServer interface {
	InvokeSync(ctx core.Context, request *protocol.RemotingCommand, timeoutMillis int64) (*protocol.RemotingCommand, error)
	InvokeAsync(ctx core.Context, request *protocol.RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error
	InvokeOneway(ctx core.Context, request *protocol.RemotingCommand, timeoutMillis int64) error
	RegisterProcessor(requestCode int32, processor RequestProcessor)
	SetDefaultProcessor(processor RequestProcessor)
	RegisterRPCHook(rpcHook RPCHook)
	SetContextEventListener(contextEventListener ContextEventListener)
	Start()
	Shutdown()
}

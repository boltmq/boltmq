package remoting

import (
	"github.com/boltmq/boltmq/net/core"
	"github.com/boltmq/common/protocol"
)

// RPCHook rpc hook, use send msg
type RPCHook interface {
	DoBeforeRequest(ctx core.Context, request *protocol.RemotingCommand)
	DoAfterResponse(ctx core.Context, request *protocol.RemotingCommand, response *protocol.RemotingCommand)
}

package remoting

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// RPCHook rpc hook, use send msg
type RPCHook interface {
	DoBeforeRequest(ctx netm.Context, request *protocol.RemotingCommand)
	DoAfterResponse(ctx netm.Context, request *protocol.RemotingCommand, response *protocol.RemotingCommand)
}

package remoting

import (
	"net"

	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// RPCHook rpc hook, use send msg
type RPCHook interface {
	DoBeforeRequest(remoteAddr string, conn net.Conn, request *protocol.RemotingCommand)
	DoAfterResponse(remoteAddr string, conn net.Conn, request *protocol.RemotingCommand, response *protocol.RemotingCommand)
}

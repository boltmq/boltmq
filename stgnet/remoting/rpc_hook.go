package remoting

import "git.oschina.net/cloudzone/smartgo/stgnet/protocol"

type RPCHook interface {
	DoBeforeRequest(remoteAddr string, request *protocol.RemotingCommand)
	DoAfterResponse(remoteAddr string, request *protocol.RemotingCommand, response *protocol.RemotingCommand)
}

package remoting

import "git.oschina.net/cloudzone/smartgo/stgnet/protocol"

type RequestProcessor interface {
	ProcessRequest(request *protocol.RemotingCommand) (*protocol.RemotingCommand, error)
}

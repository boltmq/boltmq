package remoting

import "git.oschina.net/cloudzone/smartgo/stgnet/protocol"

// RequestProcessor request processor
type RequestProcessor interface {
	ProcessRequest(request *protocol.RemotingCommand) (*protocol.RemotingCommand, error)
}

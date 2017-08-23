package remoting

import (
	"net"

	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// RequestProcessor request processor
type RequestProcessor interface {
	ProcessRequest(addr string, conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error)
}

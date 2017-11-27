package remoting

import (
	"github.com/boltmq/boltmq/net/core"
	"github.com/boltmq/common/protocol"
)

// RequestProcessor request processor
type RequestProcessor interface {
	ProcessRequest(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error)
}

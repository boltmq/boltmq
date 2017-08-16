package protocol

// RemotingCommand 服务器与客户端通过传递RemotingCommand来交互
// Author gaoyanlei
// Since 2017/8/15
const (
	RemotingVersionKey = "rocketmq.remoting.version"
	ConfigVersion      = -1
	RPC_TYPE           = 0
	RPC_ONEWAY         = 1
	version            = 1
)

type RemotingCommand struct {
	RemotingVersionKey string
	Code               int
	Opaque             int
	CustomHeader       string
	// TODO
}

func CreateResponseCommand() *RemotingCommand {
	return new(RemotingCommand)
}

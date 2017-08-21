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
	Flag               int
	// 修改字段类型 2017/8/16 Add by yintongqiang
	CustomHeader CommandCustomHeader
	Body         []byte
	Remark       string
	// TODO
}

func CreateResponseCommand() *RemotingCommand {
	return new(RemotingCommand)
}

// 创建客户端请求信息 2017/8/16 Add by yintongqiang
func CreateRequestCommand(code int, customHeader CommandCustomHeader) *RemotingCommand {
	return new(RemotingCommand)
}

func (self *RemotingCommand) IsOnewayRPC() bool {
	bits := 1 << RPC_ONEWAY
	return (self.Flag & bits) == bits
}

package protocol

import "git.oschina.net/cloudzone/smartgo/stgnet"

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
	// 修改字段类型 2017/8/16 Add by yintongqiang
	CustomHeader       stgnet.CommandCustomHeader
	// TODO
}

func CreateResponseCommand() *RemotingCommand {
	return new(RemotingCommand)
}
// 创建客户端请求信息 2017/8/16 Add by yintongqiang
func CreateRequestCommand(code int,customHeader stgnet.CommandCustomHeader) *RemotingCommand {
	return new(RemotingCommand)
}

package protocol

type RemotingCommandType int

const (
	REQUEST_COMMAND  RemotingCommandType = iota // 请求命令
	RESPONSE_COMMAND                            // 响应命令
)

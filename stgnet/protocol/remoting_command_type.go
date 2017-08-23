package protocol

type RemotingCommandType int

const (
	REQUEST_COMMAND RemotingCommandType = iota
	RESPONSE_COMMAND
)

package topic

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

type OffsetMovedEvent struct {
	ConsumerGroup string
	MessageQueue  message.MessageQueue

	// 客户端请求的Offset
	OffsetRequest int64

	//  Broker要求从这个新的Offset开始消费
	OffsetNew int64

	*protocol.RemotingSerializable
}

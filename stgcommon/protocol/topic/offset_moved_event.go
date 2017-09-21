package topic

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

type OffsetMovedEvent struct {
	ConsumerGroup string               `json:"consumerGroup"`
	MessageQueue  message.MessageQueue `json:"messageQueue"`

	// 客户端请求的Offset
	OffsetRequest int64 `json:"offsetRequest"`

	//  Broker要求从这个新的Offset开始消费
	OffsetNew int64 `json:"offsetNew"`

	*protocol.RemotingSerializable
}

func NewOffsetMovedEvent() *OffsetMovedEvent {
	event := new(OffsetMovedEvent)
	event.RemotingSerializable = new(protocol.RemotingSerializable)
	return event
}

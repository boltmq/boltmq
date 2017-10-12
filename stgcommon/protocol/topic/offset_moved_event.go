package topic

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

type OffsetMovedEvent struct {
	ConsumerGroup string               `json:"consumerGroup"` // 消费组名称
	MessageQueue  message.MessageQueue `json:"messageQueue"`  // 消息Queue
	OffsetRequest int64                `json:"offsetRequest"` // 客户端请求的Offset
	OffsetNew     int64                `json:"offsetNew"`     // Broker要求从这个新的Offset开始消费
	*protocol.RemotingSerializable
}

func NewOffsetMovedEvent() *OffsetMovedEvent {
	event := new(OffsetMovedEvent)
	event.RemotingSerializable = new(protocol.RemotingSerializable)
	return event
}

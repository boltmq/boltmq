package body

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// GetConsumerStatusBody  获得消费状态的body
// Author rongzhihong
// Since 2017/9/19
type GetConsumerStatusBody struct {
	MessageQueueTable map[*message.MessageQueue]int64            `json:"messageQueueTable"`
	ConsumerTable     map[string]map[*message.MessageQueue]int64 `json:"consumerTable"`
	*protocol.RemotingSerializable
}

func NewGetConsumerStatusBody() *GetConsumerStatusBody {
	body := new(GetConsumerStatusBody)
	body.MessageQueueTable = make(map[*message.MessageQueue]int64)
	body.ConsumerTable = make(map[string]map[*message.MessageQueue]int64)
	body.RemotingSerializable = new(protocol.RemotingSerializable)
	return body
}

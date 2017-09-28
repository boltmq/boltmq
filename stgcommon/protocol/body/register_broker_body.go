package body

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// RegisterBrokerBody 注册Broker-请求/响应体
// Author gaoyanlei
// Since 2017/8/22
type RegisterBrokerBody struct {
	TopicConfigSerializeWrapper *TopicConfigSerializeWrapper `json:"topicConfigSerializeWrapper"`
	FilterServerList            []string                     `json:"filterServerList"`
	*protocol.RemotingSerializable
}

func NewRegisterBrokerBody() *RegisterBrokerBody {
	body := new(RegisterBrokerBody)
	body.TopicConfigSerializeWrapper = NewTopicConfigSerializeWrapper()
	body.RemotingSerializable = new(protocol.RemotingSerializable)
	return body
}

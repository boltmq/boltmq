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

func NewRegisterBrokerBody(topicConfigWrapper *TopicConfigSerializeWrapper, filterServerList []string) *RegisterBrokerBody {
	body := new(RegisterBrokerBody)
	body.FilterServerList = filterServerList
	body.TopicConfigSerializeWrapper = topicConfigWrapper
	return body
}

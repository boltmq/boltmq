package body

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	set "github.com/deckarep/golang-set"
)

type TopicList struct {
	TopicList  set.Set `json:"topicList"`  // topic列表
	BrokerAddr string  `json:"brokerAddr"` // broker地址
	*protocol.RemotingSerializable
}

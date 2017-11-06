package body

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	set "github.com/deckarep/golang-set"
)

// TopicList topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/16
type TopicList struct {
	TopicList  set.Set `json:"topicList"`  // topic列表
	BrokerAddr string  `json:"brokerAddr"` // broker地址
	*protocol.RemotingSerializable
}

// NewTopicList 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/16
func NewTopicList() *TopicList {
	topicList := &TopicList{
		TopicList:            set.NewSet(),
		RemotingSerializable: new(protocol.RemotingSerializable),
	}
	return topicList
}

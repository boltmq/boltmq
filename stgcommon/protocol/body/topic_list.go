package body

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	set "github.com/deckarep/golang-set"
)

// TopicList topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/16
type TopicList struct {
	TopicList       set.Set                       `json:"topicList"`       // topic列表
	BrokerAddr      string                        `json:"brokerAddr"`      // broker地址
	TopicQueueTable map[string][]*route.QueueData `json:"topicQueueTable"` // 额外增加字段：topic路由信息
	*protocol.RemotingSerializable
}

// NewTopicList 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/16
func NewTopicList() *TopicList {
	topicList := &TopicList{
		TopicList:            set.NewSet(),
		RemotingSerializable: new(protocol.RemotingSerializable),
		TopicQueueTable:      make(map[string][]*route.QueueData),
	}
	return topicList
}

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

// TopicPlusList 拓展Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/16
type TopicPlusList struct {
	TopicList        []string                      `json:"topicList"`        // topic列表
	BrokerAddr       string                        `json:"brokerAddr"`       // broker地址
	TopicQueueTable  map[string][]*route.QueueData `json:"topicQueueTable"`  // 额外增加字段 topic<*route.QueueData>
	ClusterAddrTable map[string][]string           `json:"clusterAddrTable"` // clusterName[set<brokerName>]
	*protocol.RemotingSerializable
}

// NewTopicPlusList 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/16
func NewTopicPlusList() *TopicPlusList {
	topicPlusList := &TopicPlusList{
		TopicList:            make([]string, 0),
		TopicQueueTable:      make(map[string][]*route.QueueData),
		ClusterAddrTable:     make(map[string][]string),
		RemotingSerializable: new(protocol.RemotingSerializable),
	}
	return topicPlusList
}

type TopicUpdateConfigWapper struct {
	TopicName      string `json:"topicName"`
	ClusterName    string `json:"clusterName"`
	Order          bool   `json:"order"`
	WriteQueueNums int    `json:"writeQueueNums"`
	ReadQueueNums  int    `json:"readQueueNums"`
	BrokerAddr     string `json:"brokerAddr"`
	BrokerName     string `json:"brokerName"`
	Unit           bool   `json:"unit"`
	Perm           int    `json:"perm"`
	TopicSynFlag   int    `json:"topicSynFlag"`
}

type TopicBrokerClusterWapper struct {
	ClusterName             string                   `json:"clusterName"`
	TopicName               string                   `json:"topic"`
	TopicUpdateConfigWapper *TopicUpdateConfigWapper `json:"topicConfig"`
}

func NewTopicBrokerClusterWapper(clusterName, topicName string, queueData *route.QueueData) *TopicBrokerClusterWapper {
	topicBrokerClusterWapper := &TopicBrokerClusterWapper{
		ClusterName:             clusterName,
		TopicName:               topicName,
		TopicUpdateConfigWapper: NewTopicUpdateConfigWapper(clusterName, topicName, queueData),
	}
	return topicBrokerClusterWapper
}

func NewTopicUpdateConfigWapper(clusterName, topicName string, queueData *route.QueueData) *TopicUpdateConfigWapper {
	topicUpdateConfig := &TopicUpdateConfigWapper{
		ClusterName:    clusterName,
		TopicName:      topicName,
		Order:          false,
		Unit:           false,
		WriteQueueNums: queueData.WriteQueueNums,
		ReadQueueNums:  queueData.ReadQueueNums,
		BrokerName:     queueData.BrokerName,
		Perm:           queueData.Perm,
		TopicSynFlag:   queueData.TopicSynFlag,
	}
	return topicUpdateConfig
}

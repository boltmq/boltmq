package stgregistry

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	 set "github.com/deckarep/golang-set"
)

type RouteInfoManager struct {
	TopicQueueTable map[string][]*route.QueueData
	BrokerAddrTable map[string]*route.BrokerData
	ClusterAddrTable map[string]*set.Set


}

func (self *RouteInfoManager) getAllTopicList() {

}

func (self *RouteInfoManager) deleteTopic(topic string) {

}

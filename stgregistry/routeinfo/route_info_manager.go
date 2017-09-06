package routeinfo

import (
	nameServer "git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv/routeinfo"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	set "github.com/deckarep/golang-set"
	"sync"
)

type RouteInfoManager struct {
	TopicQueueTable   map[string][]*route.QueueData        // topic[list<QueueData>]
	BrokerAddrTable   map[string]*route.BrokerData         // brokerName[BrokerData]
	ClusterAddrTable  map[string]set.Set                   // clusterName[set<brokerName>]
	BrokerLiveTable   map[string]*routeinfo.BrokerLiveInfo // brokerAddr[brokerLiveTable]
	FilterServerTable map[string][]string                  // brokerAddr[FilterServer]
	ReadWriteLock     *sync.RWMutex                        // read & write lock
}

func NewRouteInfoManager() *RouteInfoManager {
	routeInfoManager := RouteInfoManager{
		TopicQueueTable:   make(map[string][]*route.QueueData),
		BrokerAddrTable:   make(map[string]*route.BrokerData),
		ClusterAddrTable:  make(map[string]set.Set),
		BrokerLiveTable:   make(map[string]*routeinfo.BrokerLiveInfo),
		FilterServerTable: make(map[string][]string),
		ReadWriteLock:     new(sync.RWMutex),
	}

	return &routeInfoManager
}

func (self *RouteInfoManager) getAllClusterInfo() []byte {
	clusterInfoSerializeWrapper := &body.ClusterInfo{
		BokerAddrTable:   self.BrokerAddrTable,
		ClusterAddrTable: self.ClusterAddrTable,
	}

	return clusterInfoSerializeWrapper.Encode()
}

func (self *RouteInfoManager) deleteTopic(topic string) {
	self.ReadWriteLock.Lock()
	defer self.ReadWriteLock.Unlock()
	delete(self.TopicQueueTable, topic)
}

func (self *RouteInfoManager) getAllTopicList() {

}

func (self *RouteInfoManager) registerBroker(clusterName, brokerAddr, brokerName string, brokerId int64, haServerAddr string, topicConfigWrapper body.TopicConfigSerializeWrapper, filterServerList []string, channel chan int) *nameServer.RegisterBrokerResult {
	return nil
}

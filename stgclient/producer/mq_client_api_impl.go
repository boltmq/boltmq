package producer

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
)

// MQClientAPIImpl: 内部使用核心处理api
// Author: yintongqiang
// Since:  2017/8/8

type MQClientAPIImpl struct {
	ClientRemotingProcessor *ClientRemotingProcessor
}

func NewMQClientAPIImpl(clientRemotingProcessor *ClientRemotingProcessor) *MQClientAPIImpl {

	return &MQClientAPIImpl{
		ClientRemotingProcessor:clientRemotingProcessor,
	}
}
// 调用romoting的start
func (impl *MQClientAPIImpl)Start() {

}

func (impl *MQClientAPIImpl)SendHeartbeat(addr string, heartbeatData *heartbeat.HeartbeatData, timeoutMillis int64) {

}

func (impl *MQClientAPIImpl)GetDefaultTopicRouteInfoFromNameServer(topic string, timeoutMillis int64) *route.TopicRouteData {
	return &route.TopicRouteData{}
}

func (impl *MQClientAPIImpl)GetTopicRouteInfoFromNameServer(topic string, timeoutMillis int64) *route.TopicRouteData {
	routeData:= &route.TopicRouteData{}
	routeData.QueueDatas=append(routeData.QueueDatas,&route.QueueData{BrokerName:"broker-master2",ReadQueueNums:8,WriteQueueNums:8,Perm:6,TopicSynFlag:0})
	mapBrokerAddrs:=make(map[int]string)
	mapBrokerAddrs[0]="10.128.31.124:10911"
	mapBrokerAddrs[1]="10.128.31.125:10911"
	routeData.BrokerDatas=append(routeData.BrokerDatas,&route.BrokerData{BrokerName:"broker-master2",BrokerAddrs:mapBrokerAddrs})
	return routeData
}


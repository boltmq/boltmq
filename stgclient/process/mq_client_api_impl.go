package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"strings"
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
)

// MQClientAPIImpl: 内部使用核心处理api
// Author: yintongqiang
// Since:  2017/8/8

type MQClientAPIImpl struct {
	ClientRemotingProcessor *ClientRemotingProcessor
	ProjectGroupPrefix      string
}

func NewMQClientAPIImpl(clientRemotingProcessor *ClientRemotingProcessor) *MQClientAPIImpl {

	return &MQClientAPIImpl{
		ClientRemotingProcessor:clientRemotingProcessor,
	}
}
// 调用romoting的start
func (impl *MQClientAPIImpl)Start() {
	//todo 初始化远程
	//impl.ProjectGroupPrefix

}
// 发送心跳到broker
func (impl *MQClientAPIImpl)sendHeartbeat(addr string, heartbeatData *heartbeat.HeartbeatData, timeoutMillis int64) {
	if strings.EqualFold(impl.ProjectGroupPrefix, "") {
		consumerDatas := heartbeatData.ConsumerDataSet
		for data := range consumerDatas.Iterator().C {
			consumerData := data.(heartbeat.ConsumerData)
			consumerData.GroupName = stgclient.BuildWithProjectGroup(consumerData.GroupName, impl.ProjectGroupPrefix)
			subscriptionDatas := consumerData.SubscriptionDataSet
			for subData := range subscriptionDatas.Iterator().C {
				subscriptionData := subData.(heartbeat.SubscriptionData)
				subscriptionData.Topic = stgclient.BuildWithProjectGroup(subscriptionData.Topic, impl.ProjectGroupPrefix)
			}
		}
		producerDatas := heartbeatData.ProducerDataSet
		for pData := range producerDatas.Iterator().C {
			producerData := pData.(*heartbeat.ProducerData)
			producerData.GroupName = stgclient.BuildWithProjectGroup(producerData.GroupName, impl.ProjectGroupPrefix)
		}
	}
	//todo 创建request调用invokeSync
}

func (impl *MQClientAPIImpl)GetDefaultTopicRouteInfoFromNameServer(topic string, timeoutMillis int64) *route.TopicRouteData {
	return &route.TopicRouteData{}
}

func (impl *MQClientAPIImpl)GetTopicRouteInfoFromNameServer(topic string, timeoutMillis int64) *route.TopicRouteData {
	routeData := &route.TopicRouteData{}
	routeData.QueueDatas = append(routeData.QueueDatas, &route.QueueData{BrokerName:"broker-master2", ReadQueueNums:8, WriteQueueNums:8, Perm:6, TopicSynFlag:0})
	mapBrokerAddrs := make(map[int]string)
	mapBrokerAddrs[0] = "10.128.31.124:10911"
	mapBrokerAddrs[1] = "10.128.31.125:10911"
	routeData.BrokerDatas = append(routeData.BrokerDatas, &route.BrokerData{BrokerName:"broker-master2", BrokerAddrs:mapBrokerAddrs})
	return routeData
}

func (impl *MQClientAPIImpl)SendMessage(addr string, brokerName string, msg message.Message, requestHeader header.SendMessageRequestHeader, timeoutMillis int64, communicationMode CommunicationMode, sendCallback SendCallback) SendResult {
	if !strings.EqualFold(impl.ProjectGroupPrefix, "") {
		msg.Topic = stgclient.BuildWithProjectGroup(msg.Topic, impl.ProjectGroupPrefix)
		requestHeader.ProducerGroup = stgclient.BuildWithProjectGroup(requestHeader.ProducerGroup, impl.ProjectGroupPrefix)
		requestHeader.Topic = stgclient.BuildWithProjectGroup(requestHeader.Topic, impl.ProjectGroupPrefix)
	}
	// 默认send采用v2版本
	//requestHeaderV2:=header.CreateSendMessageRequestHeaderV2(requestHeader)
	//todo  request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, requestHeaderV2)
	switch (communicationMode) {
	case ONEWAY:
	case ASYNC:
	case SYNC:
		return impl.sendMessageSync(addr, brokerName, msg, timeoutMillis)
	default:
		break
	}

	return SendResult{}
}

func (impl *MQClientAPIImpl)sendMessageSync(addr string, brokerName string, msg message.Message, timeoutMillis int64) SendResult {
	return SendResult{}
}

func (impl *MQClientAPIImpl)UpdateConsumerOffsetOneway(addr string, requestHeader header.UpdateConsumerOffsetRequestHeader, timeoutMillis int64) {
	if !strings.EqualFold(impl.ProjectGroupPrefix, "") {
		requestHeader.ConsumerGroup = stgclient.BuildWithProjectGroup(requestHeader.ConsumerGroup, impl.ProjectGroupPrefix)
		requestHeader.Topic = stgclient.BuildWithProjectGroup(requestHeader.Topic, impl.ProjectGroupPrefix)
	}
	// todo 创建request
}

func (impl *MQClientAPIImpl)GetConsumerIdListByGroup(addr string, consumerGroup string, timeoutMillis int64) []string {
	consumerGroupWithProjectGroup := consumerGroup
	if !strings.EqualFold(impl.ProjectGroupPrefix, "") {
		consumerGroupWithProjectGroup = stgclient.BuildWithProjectGroup(consumerGroup, impl.ProjectGroupPrefix)
	}
	requestHeader := header.GetConsumerListByGroupRequestHeader{ConsumerGroup:consumerGroupWithProjectGroup}
	logger.Info(requestHeader.ConsumerGroup)
	// todo 创建request
	return []string{}
}

func (impl *MQClientAPIImpl)GetMaxOffset(addr string,topic string,queueId int,timeoutMillis int64) int64 {
	topicWithProjectGroup := topic
	if !strings.EqualFold(impl.ProjectGroupPrefix, "") {
		topicWithProjectGroup = stgclient.BuildWithProjectGroup(topic, impl.ProjectGroupPrefix)
	}
	logger.Info(topicWithProjectGroup)
	// todo 创建request
	return -1
}

func (impl *MQClientAPIImpl)PullMessage(addr string,requestHeader header.PullMessageRequestHeader,
timeoutMillis int,communicationMode CommunicationMode,pullCallback consumer.PullCallback) consumer.PullResult {
	if !strings.EqualFold(impl.ProjectGroupPrefix, "") {
		requestHeader.ConsumerGroup = stgclient.BuildWithProjectGroup(requestHeader.ConsumerGroup, impl.ProjectGroupPrefix)
		requestHeader.Topic = stgclient.BuildWithProjectGroup(requestHeader.Topic, impl.ProjectGroupPrefix)
	}
	// todo 创建request
	switch communicationMode {
	case ONEWAY:
	case ASYNC:
		// todo 后续操作
	//impl.pullMessageAsync(addr, request, timeoutMillis, pullCallback)
	case SYNC:
	default:

	}
	return consumer.PullResult{}
}

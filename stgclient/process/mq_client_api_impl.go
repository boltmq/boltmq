package process

import (
	"errors"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/admin"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	util "git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	set "github.com/deckarep/golang-set"
	"strings"
)

// MQClientAPIImpl: 内部使用核心处理api
// Author: yintongqiang
// Since:  2017/8/8
type MQClientAPIImpl struct {
	DefalutRemotingClient   remoting.RemotingClient
	ClientRemotingProcessor *ClientRemotingProcessor
	ProjectGroupPrefix      string
}

func NewMQClientAPIImpl(clientRemotingProcessor *ClientRemotingProcessor) *MQClientAPIImpl {
	mClientAPIImpl := &MQClientAPIImpl{
		DefalutRemotingClient:   remoting.NewDefalutRemotingClient(),
		ClientRemotingProcessor: clientRemotingProcessor,
	}
	mClientAPIImpl.DefalutRemotingClient.RegisterProcessor(code.NOTIFY_CONSUMER_IDS_CHANGED, clientRemotingProcessor)
	return mClientAPIImpl
}

// 调用romoting的start
func (impl *MQClientAPIImpl) Start() {
	defer utils.RecoveredFn()

	impl.DefalutRemotingClient.Start()
	value, err := impl.getProjectGroupByIp(stgclient.GetLocalAddress(), 3000)
	if err != nil && !strings.EqualFold(value, "") {
		impl.ProjectGroupPrefix = value
	}

}

// 关闭romoting
func (impl *MQClientAPIImpl) Shutdwon() {
	impl.DefalutRemotingClient.Shutdown()
}

// 发送心跳到broker
func (impl *MQClientAPIImpl) sendHeartbeat(addr string, heartbeatData *heartbeat.HeartbeatData, timeoutMillis int64) error {
	if !strings.EqualFold(impl.ProjectGroupPrefix, "") {
		consumerDatas := heartbeatData.ConsumerDataSet
		for data := range consumerDatas.Iterator().C {
			consumerData := data.(heartbeat.ConsumerData)
			consumerData.GroupName = stgclient.BuildWithProjectGroup(consumerData.GroupName, impl.ProjectGroupPrefix)
			subscriptionDatas := consumerData.SubscriptionDataSet
			for subData := range subscriptionDatas.Iterator().C {
				subscriptionData := subData.(*heartbeat.SubscriptionData)
				subscriptionData.Topic = stgclient.BuildWithProjectGroup(subscriptionData.Topic, impl.ProjectGroupPrefix)
			}
		}
		producerDatas := heartbeatData.ProducerDataSet
		for pData := range producerDatas.Iterator().C {
			producerData := pData.(*heartbeat.ProducerData)
			producerData.GroupName = stgclient.BuildWithProjectGroup(producerData.GroupName, impl.ProjectGroupPrefix)
		}
	}
	request := protocol.CreateRequestCommand(code.HEART_BEAT)
	request.Body = heartbeatData.Encode()
	response, err := impl.DefalutRemotingClient.InvokeSync(addr, request, timeoutMillis)
	if response != nil && err == nil {
		switch response.Code {
		case code.SUCCESS:
		}
	} else {
		logger.Errorf("sendHeartbeat error")
	}
	return err
}

func (impl *MQClientAPIImpl) GetDefaultTopicRouteInfoFromNameServer(topic string, timeoutMillis int64) *route.TopicRouteData {
	requestHeader := namesrv.GetRouteInfoRequestHeader{Topic: topic}
	request := protocol.CreateRequestCommand(code.GET_ROUTEINTO_BY_TOPIC, &requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync("", request, timeoutMillis)
	if response != nil && err == nil {
		switch response.Code {
		case code.TOPIC_NOT_EXIST:
			logger.Warnf("get Topic [%v] RouteInfoFromNameServer is not exist value", topic)
		case code.SUCCESS:
			body := response.Body
			if len(body) > 0 {
				topicRouteData := &route.TopicRouteData{}
				topicRouteData.Decode(body)
				return topicRouteData
			}
		default:
			logger.Errorf("response code=%v,remark=%v", response.Code, response.Remark)
			panic(response.Remark)

		}
	} else {
		logger.Errorf("GetDefaultTopicRouteInfoFromNameServer topic=%v error=%v", topic, err.Error())
	}
	return nil
}

func (impl *MQClientAPIImpl) GetTopicRouteInfoFromNameServer(topic string, timeoutMillis int64) *route.TopicRouteData {
	topicWithProjectGroup := topic
	if !strings.EqualFold(impl.ProjectGroupPrefix, "") {
		topicWithProjectGroup = stgclient.BuildWithProjectGroup(topic, impl.ProjectGroupPrefix)
	}

	requestHeader := &namesrv.GetRouteInfoRequestHeader{Topic: topicWithProjectGroup}
	request := protocol.CreateRequestCommand(code.GET_ROUTEINTO_BY_TOPIC, requestHeader)

	response, err := impl.DefalutRemotingClient.InvokeSync("", request, timeoutMillis)
	if response != nil {
		switch response.Code {
		case code.SUCCESS:
			body := response.Body
			if len(body) > 0 {
				topicRouteData := &route.TopicRouteData{}
				topicRouteData.Decode(body)
				return topicRouteData
			}
		default:
			logger.Errorf("%s", response.ToString())
			//panic(response.Remark) // 新topic，client启动启动后第一次消费，获取不到重试队列topic的路由信息
		}

	} else {
		logger.Errorf("GetTopicRouteInfoFromNameServer topic=%v error=%v", topic, err.Error())
	}
	return nil
}

func (impl *MQClientAPIImpl) SendMessage(addr string, brokerName string, msg *message.Message, requestHeader header.SendMessageRequestHeader,
	timeoutMillis int64, communicationMode CommunicationMode, sendCallback SendCallback) (*SendResult, error) {
	if !strings.EqualFold(impl.ProjectGroupPrefix, "") {
		msg.Topic = stgclient.BuildWithProjectGroup(msg.Topic, impl.ProjectGroupPrefix)
		requestHeader.ProducerGroup = stgclient.BuildWithProjectGroup(requestHeader.ProducerGroup, impl.ProjectGroupPrefix)
		requestHeader.Topic = stgclient.BuildWithProjectGroup(requestHeader.Topic, impl.ProjectGroupPrefix)
	}
	// 默认send采用v2版本
	requestHeaderV2 := header.CreateSendMessageRequestHeaderV2(&requestHeader)
	request := protocol.CreateRequestCommand(code.SEND_MESSAGE_V2, requestHeaderV2)
	request.Body = msg.Body
	switch communicationMode {
	case ONEWAY:
		request.MarkOnewayRPC()
		impl.DefalutRemotingClient.InvokeOneway(addr, request, timeoutMillis)
		return nil, nil
	case ASYNC:
		impl.sendMessageASync(addr, brokerName, msg, timeoutMillis, request, sendCallback)
		return nil, nil
	case SYNC:
		return impl.sendMessageSync(addr, brokerName, msg, timeoutMillis, request)
	default:
		break
	}

	return nil, errors.New("SendMessage error")
}

func (impl *MQClientAPIImpl) sendMessageSync(addr string, brokerName string, msg *message.Message, timeoutMillis int64, request *protocol.RemotingCommand) (*SendResult, error) {
	response, err := impl.DefalutRemotingClient.InvokeSync(addr, request, timeoutMillis)
	if err != nil {
		logger.Errorf("sendMessageSync err: %s, the request is %s", err.Error(), request.ToString())
		return nil, err
	}
	return impl.processSendResponse(brokerName, msg, response)
}

func (impl *MQClientAPIImpl) sendMessageASync(addr string, brokerName string, msg *message.Message, timeoutMillis int64, request *protocol.RemotingCommand, callback SendCallback) error {
	return impl.DefalutRemotingClient.InvokeAsync(addr, request, timeoutMillis, func(responseFuture *remoting.ResponseFuture) {
		if responseFuture == nil || callback == nil {
			return
		}
		response := responseFuture.GetRemotingCommand()
		sendResult, err := impl.processSendResponse(brokerName, msg, response)
		callback(sendResult, err)
	})
}

// 处理发送消息响应
func (impl *MQClientAPIImpl) processSendResponse(brokerName string, msg *message.Message, response *protocol.RemotingCommand) (*SendResult, error) {
	if response != nil {
		switch response.Code {
		case code.FLUSH_DISK_TIMEOUT:
			fallthrough
		case code.FLUSH_SLAVE_TIMEOUT:
			fallthrough
		case code.SLAVE_NOT_AVAILABLE:
			logger.Warnf("brokerName %v %s", brokerName, code.ParseResponse(response.Code))
			fallthrough
		case code.SUCCESS:
			sendStatus := SEND_OK
			switch response.Code {
			case code.FLUSH_DISK_TIMEOUT:
				sendStatus = FLUSH_DISK_TIMEOUT
			case code.FLUSH_SLAVE_TIMEOUT:
				sendStatus = FLUSH_SLAVE_TIMEOUT
			case code.SLAVE_NOT_AVAILABLE:
				sendStatus = SLAVE_NOT_AVAILABLE
			case code.SUCCESS:
				sendStatus = SEND_OK
			default:
				return nil, errors.New("processSendResponse error=" + response.Remark)
			}
			responseHeader := &header.SendMessageResponseHeader{}
			response.DecodeCommandCustomHeader(responseHeader)
			messageQueue := &message.MessageQueue{Topic: msg.Topic, BrokerName: brokerName, QueueId: int(responseHeader.QueueId)}
			sendResult := NewSendResult(sendStatus, responseHeader.MsgId, messageQueue, responseHeader.QueueOffset, impl.ProjectGroupPrefix)
			sendResult.TransactionId = responseHeader.TransactionId
			return sendResult, nil
		}
	} else {
		return nil, errors.New("processSendResponse error response is nil")
	}
	if response != nil {
		return nil, errors.New("processSendResponse error=" + response.Remark)
	}
	return nil, errors.New("processSendResponse error")
}

func (impl *MQClientAPIImpl) UpdateConsumerOffsetOneway(addr string, requestHeader header.UpdateConsumerOffsetRequestHeader, timeoutMillis int64) {
	if !strings.EqualFold(impl.ProjectGroupPrefix, "") {
		requestHeader.ConsumerGroup = stgclient.BuildWithProjectGroup(requestHeader.ConsumerGroup, impl.ProjectGroupPrefix)
		requestHeader.Topic = stgclient.BuildWithProjectGroup(requestHeader.Topic, impl.ProjectGroupPrefix)
	}
	request := protocol.CreateRequestCommand(code.UPDATE_CONSUMER_OFFSET, &requestHeader)
	// oneway 特殊处理
	request.MarkOnewayRPC()
	impl.DefalutRemotingClient.InvokeOneway(addr, request, timeoutMillis)
}

func (impl *MQClientAPIImpl) GetConsumerIdListByGroup(addr string, consumerGroup string, timeoutMillis int64) []string {
	consumerGroupWithProjectGroup := consumerGroup
	if !strings.EqualFold(impl.ProjectGroupPrefix, "") {
		consumerGroupWithProjectGroup = stgclient.BuildWithProjectGroup(consumerGroup, impl.ProjectGroupPrefix)
	}
	requestHeader := header.GetConsumerListByGroupRequestHeader{ConsumerGroup: consumerGroupWithProjectGroup}
	request := protocol.CreateRequestCommand(code.GET_CONSUMER_LIST_BY_GROUP, &requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(addr, request, timeoutMillis)
	if response != nil && err == nil {
		switch response.Code {
		case code.SUCCESS:
			if len(response.Body) > 0 {
				responseBody := &header.GetConsumerListByGroupResponseBody{}
				responseBody.Decode(response.Body)
				return responseBody.ConsumerIdList
			}
		}
	}
	return []string{}
}

// 获取队列最大offset
func (impl *MQClientAPIImpl) GetMaxOffset(addr string, topic string, queueId int, timeoutMillis int64) int64 {
	topicWithProjectGroup := topic
	if !strings.EqualFold(impl.ProjectGroupPrefix, "") {
		topicWithProjectGroup = stgclient.BuildWithProjectGroup(topic, impl.ProjectGroupPrefix)
	}
	requestHeader := header.GetMaxOffsetRequestHeader{Topic: topicWithProjectGroup, QueueId: queueId}
	request := protocol.CreateRequestCommand(code.GET_MAX_OFFSET, &requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(addr, request, timeoutMillis)
	if response != nil && err == nil {
		switch response.Code {
		case code.SUCCESS:
			responseHeader := &header.GetMaxOffsetResponseHeader{}
			response.DecodeCommandCustomHeader(responseHeader)
			return responseHeader.Offset
		}
	} else {
		logger.Errorf("getMaxOffset error")
	}
	return -1
}

func (impl *MQClientAPIImpl) PullMessage(addr string, requestHeader header.PullMessageRequestHeader,
	timeoutMillis int, communicationMode CommunicationMode, pullCallback PullCallback) *PullResultExt {
	if !strings.EqualFold(impl.ProjectGroupPrefix, "") {
		requestHeader.ConsumerGroup = stgclient.BuildWithProjectGroup(requestHeader.ConsumerGroup, impl.ProjectGroupPrefix)
		requestHeader.Topic = stgclient.BuildWithProjectGroup(requestHeader.Topic, impl.ProjectGroupPrefix)
	}
	request := protocol.CreateRequestCommand(code.PULL_MESSAGE, &requestHeader)
	switch communicationMode {
	case ONEWAY:
	case ASYNC:
		//fmt.Println(time.Now().Unix(), requestHeader.Topic, requestHeader.QueueId, requestHeader.QueueOffset, "-------------------------------------")
		impl.pullMessageAsync(addr, request, timeoutMillis, pullCallback)
	case SYNC:
		return impl.pullMessageSync(addr, request, timeoutMillis)
	default:

	}
	return nil
}

func (impl *MQClientAPIImpl) queryConsumerOffset(addr string, requestHeader header.QueryConsumerOffsetRequestHeader, timeoutMillis int64) int64 {
	if !strings.EqualFold(impl.ProjectGroupPrefix, "") {
		requestHeader.ConsumerGroup = stgclient.BuildWithProjectGroup(requestHeader.ConsumerGroup, impl.ProjectGroupPrefix)
		requestHeader.Topic = stgclient.BuildWithProjectGroup(requestHeader.Topic, impl.ProjectGroupPrefix)
	}
	request := protocol.CreateRequestCommand(code.QUERY_CONSUMER_OFFSET, &requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(addr, request, timeoutMillis)
	if err != nil {
		logger.Errorf("topic=%v queueId=%v queryConsumerOffset error=%v", requestHeader.Topic, requestHeader.QueueId, err.Error())
	}
	if response != nil && err == nil {
		switch response.Code {
		case code.SUCCESS:
			responseHeader := &header.QueryConsumerOffsetResponseHeader{}
			response.DecodeCommandCustomHeader(responseHeader)
			return responseHeader.Offset
		}
	}
	return -1
}

func (impl *MQClientAPIImpl) pullMessageSync(addr string, request *protocol.RemotingCommand, timeoutMillis int) *PullResultExt {
	response, err := impl.DefalutRemotingClient.InvokeSync(addr, request, int64(timeoutMillis))
	if err != nil {
		logger.Errorf("pullMessageSync error=%v", err.Error())
	}
	if response != nil {
		return impl.processPullResponse(response)
	}
	return nil
}

func (impl *MQClientAPIImpl) UpdateNameServerAddressList(addrs string) {
	impl.DefalutRemotingClient.UpdateNameServerAddressList(strings.Split(addrs, ";"))
}

func (impl *MQClientAPIImpl) consumerSendMessageBack(addr string, msg *message.MessageExt, consumerGroup string, delayLevel int, timeoutMillis int) {
	consumerGroupWithProjectGroup := consumerGroup
	if !strings.EqualFold(impl.ProjectGroupPrefix, "") {
		consumerGroupWithProjectGroup = stgclient.BuildWithProjectGroup(consumerGroup, impl.ProjectGroupPrefix)
		msg.Topic = stgclient.BuildWithProjectGroup(msg.Topic, impl.ProjectGroupPrefix)
	}
	requestHeader := header.ConsumerSendMsgBackRequestHeader{
		Group:       consumerGroupWithProjectGroup,
		OriginTopic: msg.Topic,
		Offset:      msg.CommitLogOffset,
		DelayLevel:  int32(delayLevel),
		OriginMsgId: msg.MsgId,
	}
	request := protocol.CreateRequestCommand(code.CONSUMER_SEND_MSG_BACK, &requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(addr, request, int64(timeoutMillis))
	if response != nil && err == nil {
		switch response.Code {
		case code.SUCCESS:
		default:
			break
		}
	} else {
		logger.Errorf("consumerSendMessageBack error")
	}

}

func (impl *MQClientAPIImpl) pullMessageAsync(addr string, request *protocol.RemotingCommand, timeoutMillis int, pullCallback PullCallback) {
	invokeCallback := func(responseFuture *remoting.ResponseFuture) {
		response := responseFuture.GetRemotingCommand()
		if response != nil {
			pullResultExt := impl.processPullResponse(response)
			pullCallback.OnSuccess(pullResultExt)
		} else {
			if !responseFuture.IsSendRequestOK() {
				logger.Warnf("send request not ok")
			} else if responseFuture.IsTimeout() {
				logger.Warnf("send request time out")
			} else {
				logger.Warnf("send request fail")
			}
		}
	}
	impl.DefalutRemotingClient.InvokeAsync(addr, request, int64(timeoutMillis), invokeCallback)
}

func (impl *MQClientAPIImpl) unRegisterClient(addr, clientID, producerGroup, consumerGroup string, timeoutMillis int) {
	producerGroupWithProjectGroup := producerGroup
	consumerGroupWithProjectGroup := consumerGroup
	if !strings.EqualFold("", impl.ProjectGroupPrefix) {
		producerGroupWithProjectGroup = stgclient.BuildWithProjectGroup(producerGroup, impl.ProjectGroupPrefix)
		consumerGroupWithProjectGroup = stgclient.BuildWithProjectGroup(consumerGroup, impl.ProjectGroupPrefix)
	}
	requestHeader := header.UnregisterClientRequestHeader{ClientID: clientID, ProducerGroup: producerGroupWithProjectGroup, ConsumerGroup: consumerGroupWithProjectGroup}
	request := protocol.CreateRequestCommand(code.UNREGISTER_CLIENT, &requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(addr, request, int64(timeoutMillis))
	if response != nil && err == nil {
		switch response.Code {
		case code.SUCCESS:
		default:
		}
	} else {
		logger.Errorf("unRegisterClient error")
	}
}

func (impl *MQClientAPIImpl) processPullResponse(response *protocol.RemotingCommand) *PullResultExt {
	pullStatus := consumer.NO_NEW_MSG
	switch response.Code {
	case code.SUCCESS:
		pullStatus = consumer.FOUND
	case code.PULL_NOT_FOUND:
		pullStatus = consumer.NO_NEW_MSG
	case code.PULL_RETRY_IMMEDIATELY:
		pullStatus = consumer.NO_MATCHED_MSG
	case code.PULL_OFFSET_MOVED:
		pullStatus = consumer.OFFSET_ILLEGAL
	}
	reponseHeader := &header.PullMessageResponseHeader{}
	response.DecodeCommandCustomHeader(reponseHeader)
	return NewPullResultExt(pullStatus, reponseHeader.NextBeginOffset,
		reponseHeader.MinOffset, reponseHeader.MaxOffset, nil, reponseHeader.SuggestWhichBrokerId, response.Body)
}

// 创建topic
func (impl *MQClientAPIImpl) CreateTopic(addr, defaultTopic string, topicConfig stgcommon.TopicConfig, timeoutMillis int) {
	topicWithProjectGroup := topicConfig.TopicName
	if !strings.EqualFold("", impl.ProjectGroupPrefix) {
		topicWithProjectGroup = stgclient.BuildWithProjectGroup(topicConfig.TopicName, impl.ProjectGroupPrefix)
	}
	requestHeader := header.CreateTopicRequestHeader{Topic: topicWithProjectGroup,
		DefaultTopic: defaultTopic, ReadQueueNums: topicConfig.ReadQueueNums, WriteQueueNums: topicConfig.WriteQueueNums,
		TopicFilterType: topicConfig.TopicFilterType, TopicSysFlag: topicConfig.TopicSysFlag, Order: topicConfig.Order,
		Perm: topicConfig.Perm}
	request := protocol.CreateRequestCommand(code.UPDATE_AND_CREATE_TOPIC, &requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(addr, request, int64(timeoutMillis))
	if response != nil && err == nil {
		switch response.Code {
		case code.SUCCESS:
		default:
		}
	} else {
		logger.Errorf("createTopic error", err.Error())
	}

}

// 从namesrv查询客户端IP信息
func (impl *MQClientAPIImpl) getProjectGroupByIp(ip string, timeoutMillis int64) (string, error) {
	return impl.getKVConfigValue(util.NAMESPACE_PROJECT_CONFIG, ip, timeoutMillis)
}

// 获取配置信息
func (impl *MQClientAPIImpl) getKVConfigValue(namespace, key string, timeoutMillis int64) (string, error) {
	requestHeader := &namesrv.GetKVConfigRequestHeader{Namespace: namespace, Key: key}
	request := protocol.CreateRequestCommand(code.GET_KV_CONFIG, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync("", request, timeoutMillis)
	if response != nil && err == nil {
		switch response.Code {
		case code.SUCCESS:
			responseHeader := &namesrv.GetKVConfigResponseHeader{}
			response.DecodeCommandCustomHeader(responseHeader)
			return responseHeader.Value, err
		default:

		}
	} else {
		return "", errors.New("invokesync error=" + err.Error())
	}
	return "", nil
}

// GetTopicListFromNameServer 从Namesrv查询所有Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
func (impl *MQClientAPIImpl) GetTopicListFromNameServer(timeoutMills int64) (*body.TopicList, error) {
	topicList := new(body.TopicList)
	request := protocol.CreateRequestCommand(code.GET_ALL_TOPIC_LIST_FROM_NAMESERVER)
	response, err := impl.DefalutRemotingClient.InvokeSync("", request, timeoutMills)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetTopicListFromNameServer response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetTopicListFromNameServer failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	content := response.Body
	if content == nil || len(content) == 0 {
		return topicList, nil
	}

	err = topicList.CustomDecode(content, topicList)
	if err != nil {
		return nil, err
	}
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) && topicList.TopicList != nil {
		newTopicSet := set.NewSet()
		for topic := range topicList.TopicList.Iterator().C {
			newTopicSet.Add(stgclient.ClearProjectGroup(topic.(string), impl.ProjectGroupPrefix))
		}
		topicList.TopicList = newTopicSet
	}
	return topicList, nil
}

// GetTopicStatsInfo 查询Topic状态信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetTopicStatsInfo(brokerAddr, topic string, timeoutMillis int64) (*admin.TopicStatsTable, error) {
	topicWithProjectGroup := topic
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) {
		topicWithProjectGroup = stgclient.BuildWithProjectGroup(topic, impl.ProjectGroupPrefix)
	}
	requestHeader := header.NewGetTopicStatsInfoRequestHeader(topicWithProjectGroup)
	request := protocol.CreateRequestCommand(code.GET_TOPIC_STATS_INFO, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetTopicStatsInfo response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetTopicStatsInfo failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	topicStatsTable := new(admin.TopicStatsTable)
	content := response.Body
	if content == nil || len(content) == 0 {
		return topicStatsTable, nil
	}

	err = topicStatsTable.CustomDecode(content, topicStatsTable)
	if err != nil {
		return nil, err
	}
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) && topicStatsTable.OffsetTable != nil {
		newTopicOffsetMap := make(map[*message.MessageQueue]*admin.TopicOffset, 256)
		for key, value := range topicStatsTable.OffsetTable {
			if key != nil {
				key.Topic = stgclient.ClearProjectGroup(key.Topic, impl.ProjectGroupPrefix)
				newTopicOffsetMap[key] = value
			}
		}
		topicStatsTable.OffsetTable = newTopicOffsetMap
	}
	return topicStatsTable, nil
}

// GetConsumeStats 查询消费者状态
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetConsumeStatsByTopic(brokerAddr, consumerGroup, topic string, timeoutMillis int64) (*admin.ConsumeStats, error) {
	consumerGroupWithProjectGroup := consumerGroup
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) {
		consumerGroupWithProjectGroup = stgclient.BuildWithProjectGroup(consumerGroup, impl.ProjectGroupPrefix)
	}
	requestHeader := header.NewGetConsumeStatsRequestHeader(consumerGroupWithProjectGroup, topic)
	request := protocol.CreateRequestCommand(code.GET_CONSUME_STATS, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetConsumeStatsByTopic response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetConsumeStats failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}

	consumeStats := new(admin.ConsumeStats)
	content := response.Body
	if content == nil || len(content) == 0 {
		return consumeStats, nil
	}

	err = consumeStats.CustomDecode(content, consumeStats)
	if err != nil {
		return nil, err
	}
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) && consumeStats.OffsetTable != nil {
		newTopicOffsetMap := make(map[*message.MessageQueue]*admin.OffsetWrapper, 256)
		for key, value := range consumeStats.OffsetTable {
			if key != nil {
				key.Topic = stgclient.ClearProjectGroup(key.Topic, impl.ProjectGroupPrefix)
				newTopicOffsetMap[key] = value
			}
		}
		consumeStats.OffsetTable = newTopicOffsetMap
	}
	return consumeStats, nil
}

// GetConsumeStats 查询消费者状态
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetConsumeStats(brokerAddr, consumerGroup string, timeoutMillis int64) (*admin.ConsumeStats, error) {
	return impl.GetConsumeStatsByTopic(brokerAddr, consumerGroup, "", timeoutMillis)
}

// GetProducerConnectionList 查询在线生产者进程信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetProducerConnectionList(brokerAddr, producerGroup string, timeoutMillis int64) (*body.ProducerConnection, error) {
	producerGroupWithProjectGroup := producerGroup
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) {
		producerGroupWithProjectGroup = stgclient.BuildWithProjectGroup(producerGroup, impl.ProjectGroupPrefix)
	}
	requestHeader := header.NewGetProducerConnectionListRequestHeader(producerGroupWithProjectGroup)
	request := protocol.CreateRequestCommand(code.GET_PRODUCER_CONNECTION_LIST, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetProducerConnectionList response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetProducerConnectionList failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	producerConnection := &body.ProducerConnection{}
	err = producerConnection.CustomDecode(response.Body, producerConnection)
	return producerConnection, err
}

// GetConsumerConnectionList 查询在线消费进程列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetConsumerConnectionList(brokerAddr, consumerGroup string, timeoutMillis int64) (*header.ConsumerConnection, error) {
	consumerGroupWithProjectGroup := consumerGroup
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) {
		consumerGroupWithProjectGroup = stgclient.BuildWithProjectGroup(consumerGroup, impl.ProjectGroupPrefix)
	}
	requestHeader := header.NewGetConsumerConnectionListRequestHeader(consumerGroupWithProjectGroup)
	request := protocol.CreateRequestCommand(code.GET_CONSUMER_CONNECTION_LIST, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetConsumerConnectionList response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetConsumerConnectionList failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}

	consumerConnection := new(header.ConsumerConnection)
	content := response.Body
	if content == nil || len(content) == 0 {
		return consumerConnection, nil
	}

	err = consumerConnection.CustomDecode(content, consumerConnection)
	if err != nil {
		return nil, err
	}
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) && consumerConnection != nil && consumerConnection.SubscriptionTable != nil {
		subscriptionDataConcurrentHashMap := consumerConnection.SubscriptionTable
		for subscriptionDataEntry := subscriptionDataConcurrentHashMap.Iterator(); subscriptionDataEntry.HasNext(); {
			key, value, _ := subscriptionDataEntry.Next()
			if key == nil || value == nil {
				continue
			}
			topic := key.(string)
			subscriptionData, ok := value.(*heartbeat.SubscriptionData)
			if ok {
				subscriptionData.Topic = stgclient.ClearProjectGroup(topic, impl.ProjectGroupPrefix)
				subscriptionDataConcurrentHashMap.Put(topic, subscriptionData)
			} else {
				logger.Warnf("GetConsumerConnectionList err: %v", subscriptionData)
			}
		}
	}
	return consumerConnection, nil
}

// GetBrokerRuntimeInfo 查询Broker运行时状态信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetBrokerRuntimeInfo(brokerAddr string, timeoutMillis int64) (*body.KVTable, error) {
	request := protocol.CreateRequestCommand(code.GET_BROKER_RUNTIME_INFO)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetBrokerRuntimeInfo response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetBrokerRuntimeInfo failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	kvTable := new(body.KVTable)
	err = kvTable.CustomDecode(response.Body, kvTable)
	return kvTable, err
}

// GetBrokerClusterInfo 查询Cluster集群信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetBrokerClusterInfo(timeoutMillis int64) (*body.ClusterInfo, error) {
	request := protocol.CreateRequestCommand(code.GET_BROKER_CLUSTER_INFO)
	response, err := impl.DefalutRemotingClient.InvokeSync("", request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetBrokerClusterInfo response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetBrokerClusterInfo failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	clusterInfo := new(body.ClusterInfo)
	err = clusterInfo.CustomDecode(response.Body, clusterInfo)
	return clusterInfo, err
}

// WipeWritePermOfBroker 关闭broker写权限
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) WipeWritePermOfBroker(namesrvAddr, brokerName string, timeoutMillis int64) (int, error) {
	requestHeader := namesrv.WipeWritePermOfBrokerRequestHeader{BrokerName: brokerName}
	request := protocol.CreateRequestCommand(code.WIPE_WRITE_PERM_OF_BROKER, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(namesrvAddr, request, timeoutMillis)
	if err != nil {
		return 0, err
	}
	if response == nil {
		return 0, fmt.Errorf("WipeWritePermOfBroker response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("WipeWritePermOfBroker failed. %s", response.ToString())
		return 0, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	responseHeader := &namesrv.WipeWritePermOfBrokerResponseHeader{}
	err = response.DecodeCommandCustomHeader(responseHeader)
	if err != nil {
		return 0, err
	}
	return responseHeader.WipeTopicCount, nil
}

// DeleteTopicInBroker 删除broker节点上对应的Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) DeleteTopicInBroker(brokerAddr, topic string, timeoutMillis int64) error {
	topicWithProjectGroup := topic
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) {
		topicWithProjectGroup = stgclient.BuildWithProjectGroup(topic, impl.ProjectGroupPrefix)
	}
	requestHeader := header.DeleteTopicRequestHeader{Topic: topicWithProjectGroup}
	request := protocol.CreateRequestCommand(code.DELETE_TOPIC_IN_BROKER, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return err
	}
	if response == nil {
		return fmt.Errorf("DeleteTopicInBroker response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("DeleteTopicInBroker failed. %s", response.ToString())
		return fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	return nil
}

// DeleteTopicInNameServer 删除Namesrv维护的Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) DeleteTopicInNameServer(namesrvAddr, topic string, timeoutMillis int64) error {
	topicWithProjectGroup := topic
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) {
		topicWithProjectGroup = stgclient.BuildWithProjectGroup(topic, impl.ProjectGroupPrefix)
	}
	requestHeader := &header.DeleteTopicRequestHeader{Topic: topicWithProjectGroup}
	request := protocol.CreateRequestCommand(code.DELETE_TOPIC_IN_NAMESRV, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(namesrvAddr, request, timeoutMillis)
	if err != nil {
		return err
	}
	if response == nil {
		return fmt.Errorf("DeleteTopicInNameServer response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("DeleteTopicInNameServer failed. %s", response.ToString())
		return fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	return nil
}

// DeleteSubscriptionGroup 删除订阅组信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) DeleteSubscriptionGroup(brokerAddr, groupName string, timeoutMillis int64) error {
	groupWithProjectGroup := groupName
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) {
		groupWithProjectGroup = stgclient.BuildWithProjectGroup(groupName, impl.ProjectGroupPrefix)
	}
	requestHeader := header.DeleteSubscriptionGroupRequestHeader{GroupName: groupWithProjectGroup}
	request := protocol.CreateRequestCommand(code.DELETE_SUBSCRIPTIONGROUP, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return err
	}
	if response == nil {
		return fmt.Errorf("DeleteSubscriptionGroup response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("DeleteSubscriptionGroup failed. %s", response.ToString())
		return fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	return nil
}

// InvokeBrokerToGetConsumerStatus 反向查找broker中的consume状态
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) InvokeBrokerToGetConsumerStatus(brokerAddr, topic, group, clientAddr string, timeoutMillis int64) (map[string]map[*message.MessageQueue]int64, error) {
	requestHeader := header.NewGetConsumerStatusRequestHeader(topic, group, clientAddr)
	request := protocol.CreateRequestCommand(code.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("InvokeBrokerToGetConsumerStatus response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("InvokeBrokerToGetConsumerStatus failed. %s", response.ToString())
		return fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	consumerStatusBody := &body.GetConsumerStatusBody{}
	err = consumerStatusBody.CustomDecode(response.Body, consumerStatusBody)
	return consumerStatusBody, err
}

// QueryTopicConsumeByWho 查询topic被那些组消费
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) QueryTopicConsumeByWho(brokerAddr, topic string, timeoutMillis int64) (*body.GroupList, error) {
	requestHeader := header.QueryTopicConsumeByWhoRequestHeader{Topic: topic}
	request := protocol.CreateRequestCommand(code.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("QueryTopicConsumeByWho response is nil")
	}
	if response != code.SUCCESS {
		logger.Errorf("QueryTopicConsumeByWho failed. %s", response.ToString())
		return fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	groupList := &body.GroupList{}
	err = groupList.CustomDecode(response.Body, groupList)
	return groupList, err
}

// GetTopicsByCluster 查询集群信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetTopicsByCluster(cluster string, timeoutMillis int64) (*body.TopicList, error) {
	requestHeader := header.GetTopicsByClusterRequestHeader{Cluster: cluster}
	request := protocol.CreateRequestCommand(code.GET_TOPICS_BY_CLUSTER, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync("", request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetTopicsByCluster response is nil")
	}
	if response != code.SUCCESS {
		logger.Errorf("GetTopicsByCluster failed. %s", response.ToString())
		return fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	topicList := &body.TopicList{}
	err = topicList.CustomDecode(response.Body, topicList)
	if err != nil {
		return nil, err
	}
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) && topicList.TopicList != nil {
		newTopicSet := set.NewSet()
		for itor := range topicList.TopicList.Iterator().C {
			if topic, ok := itor.(string); ok {
				newTopicSet.Add(stgclient.ClearProjectGroup(topic, impl.ProjectGroupPrefix))
			}
		}
		topicList.TopicList = newTopicSet
	}
	return topicList, nil
}

// GetConsumerRunningInfo 获得consumer运行时状态信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetConsumerRunningInfo(brokerAddr, consumerGroup, clientId string, jstack bool, timeoutMillis int64) (*body.ConsumerRunningInfo, error) {
	//TODO：

	return nil, nil
}

// ViewBrokerStatsData 查询broker节点自身的状态信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) ViewBrokerStatsData(brokerAddr, statsName, statsKey string, timeoutMillis int64) (*body.BrokerStatsData, error) {
	requestHeader := &header.ViewBrokerStatsDataRequestHeader{StatsName: statsName, StatsKey: statsKey}
	request := protocol.CreateRequestCommand(code.VIEW_BROKER_STATS_DATA, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetTopicsByCluster response is nil")
	}
	if response != code.SUCCESS {
		logger.Errorf("GetTopicsByCluster failed. %s", response.ToString())
		return fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	brokerStatsData := &body.BrokerStatsData{}
	err = brokerStatsData.CustomDecode(response.Body, brokerStatsData)
	if err != nil {
		return nil, err
	}
	return brokerStatsData, err
}

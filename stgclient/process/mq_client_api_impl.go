package process

import (
	"errors"
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	util "git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"strings"
	cprotocol"git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"fmt"
	"time"
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
	mClientAPIImpl.DefalutRemotingClient.RegisterProcessor(cprotocol.NOTIFY_CONSUMER_IDS_CHANGED, clientRemotingProcessor)
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
	request := protocol.CreateRequestCommand(code.HEART_BEAT, nil)
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
			logger.Errorf("response code=%v,remark=%v", response.Code, response.Remark)
			//panic(response.Remark)
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
		case code.FLUSH_SLAVE_TIMEOUT:
		case code.SLAVE_NOT_AVAILABLE:
			logger.Warnf("brokerName %v SLAVE_NOT_AVAILABLE", brokerName)
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
		fmt.Println(time.Now().Unix(),requestHeader.Topic, requestHeader.QueueId, requestHeader.QueueOffset, "-------------------------------------")
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
		reponseHeader.MaxOffset, reponseHeader.MaxOffset, nil, reponseHeader.SuggestWhichBrokerId, response.Body)
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

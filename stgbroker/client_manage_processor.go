package stgbroker

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker/client"
	"git.oschina.net/cloudzone/smartgo/stgbroker/mqtrace"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/listener"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	protocol2 "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

type ClientManageProcessor struct {
	BrokerController       *BrokerController
	consumeMessageHookList []mqtrace.ConsumeMessageHook
}

// NewClientManageProcessor 初始化ClientManageProcessor
// Author gaoyanlei
// Since 2017/8/9
func NewClientManageProcessor(brokerController *BrokerController) *ClientManageProcessor {
	var clientManageProcessor = new(ClientManageProcessor)
	clientManageProcessor.BrokerController = brokerController
	return clientManageProcessor
}

func (cmp *ClientManageProcessor) ProcessRequest(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	switch request.Code {
	case protocol2.HEART_BEAT:
		return cmp.heartBeat(ctx, request)
	case protocol2.UNREGISTER_CLIENT:
		return cmp.unregisterClient(ctx, request)
	case protocol2.GET_CONSUMER_LIST_BY_GROUP:
		return cmp.getConsumerListByGroup(ctx, request)
	case protocol2.QUERY_CONSUMER_OFFSET:
		return cmp.queryConsumerOffset(ctx, request)
	case protocol2.UPDATE_CONSUMER_OFFSET:
		return cmp.updateConsumerOffset(ctx, request)
	}
	return nil, nil
}

// heartBeat 心跳服务
// Author gaoyanlei
// Since 2017/8/23
func (cmp *ClientManageProcessor) heartBeat(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}

	heartbeatData := &heartbeat.HeartbeatData{}
	heartbeatData.Decode(request.Body)
	consumerDataSet := heartbeatData.ConsumerDataSet

	channelInfo := client.NewClientChannelInfo(ctx, heartbeatData.ClientID, request.Language, ctx.LocalAddr().String(), request.Version)

	for value := range consumerDataSet.Iterator().C {
		if consumerData, ok := value.(*heartbeat.ConsumerData); ok {
			subscriptionGroupConfig :=
				cmp.BrokerController.SubscriptionGroupManager.findSubscriptionGroupConfig(consumerData.GroupName)

			if subscriptionGroupConfig != nil {
				topicSysFlag := 0
				if consumerData.UnitMode {
					topicSysFlag = sysflag.TopicBuildSysFlag(false, true)
				}

				newTopic := stgcommon.GetRetryTopic(consumerData.GroupName)
				cmp.BrokerController.TopicConfigManager.createTopicInSendMessageBackMethod( //
					newTopic, //
					subscriptionGroupConfig.RetryQueueNums, //
					constant.PERM_WRITE|constant.PERM_READ, topicSysFlag)
			}

			changed := cmp.BrokerController.ConsumerManager.RegisterConsumer(consumerData.GroupName, ctx,
				consumerData.ConsumeType, consumerData.MessageModel, consumerData.ConsumeFromWhere, consumerData.SubscriptionDataSet)
			if changed {
				logger.Infof("registerConsumer info changed {} %s", consumerData.ToString(), ctx.RemoteAddr().String())
			}
		}
	}

	// 注册Producer
	for value := range heartbeatData.ProducerDataSet.Iterator().C {
		if producerData, ok := value.(*heartbeat.ProducerData); ok {
			cmp.BrokerController.ProducerManager.RegisterProducer(producerData.GroupName, channelInfo)
		}
	}

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// unregisterClient 注销客户端
// Author gaoyanlei
// Since 2017/8/24
func (cmp *ClientManageProcessor) unregisterClient(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}

	requestHeader := &header.UnregisterClientRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	channelInfo := client.NewClientChannelInfo(ctx, requestHeader.ClientID, request.Language, ctx.LocalAddr().String(), request.Version)

	// 注销Producer
	{
		group := requestHeader.ProducerGroup
		if group != "" {
			cmp.BrokerController.ProducerManager.UnregisterProducer(group, channelInfo)
		}
	}

	// 注销Consumer
	{
		group := requestHeader.ConsumerGroup
		if group != "" {
			cmp.BrokerController.ConsumerManager.UnregisterConsumer(group, channelInfo)
		}
	}

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// queryConsumerOffset  查询Consumer的偏移量
// Author rongzhihong
// Since 2017/9/14
func (cmp *ClientManageProcessor) queryConsumerOffset(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	responseHeader := &header.QueryConsumerOffsetResponseHeader{}
	response.CustomHeader = responseHeader

	requestHeader := &header.QueryConsumerOffsetRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	offset := cmp.BrokerController.ConsumerOffsetManager.queryOffset(requestHeader.ConsumerGroup, requestHeader.Topic, int(requestHeader.QueueId))

	// 订阅组存在
	if offset >= 0 {
		responseHeader.Offset = offset
		response.Code = code.SUCCESS
		response.Remark = ""
	} else { // 订阅组不存在

		// TODO minOffset := cmp.BrokerController.MessageStore.getMinOffsetInQuque(requestHeader.Topic, requestHeader.QueueId)
		minOffset := int64(0)
		// TODO isInDisk := cmp.BrokerController.MessageStore.checkInDiskByConsumeOffset(requestHeader.Topic, requestHeader.QueueId, 0)
		isInDisk := false
		// 订阅组不存在情况下，如果这个队列的消息最小Offset是0，则表示这个Topic上线时间不长，服务器堆积的数据也不多，那么这个订阅组就从0开始消费。
		// 尤其对于Topic队列数动态扩容时，必须要从0开始消费。
		if minOffset <= 0 && !isInDisk {
			responseHeader.Offset = 0
			response.Code = code.SUCCESS
			response.Remark = ""
		} else {
			response.Code = code.QUERY_NOT_FOUND
			response.Remark = "Not found, maybe this group consumer boot first"
		}
	}

	return response, nil
}

// updateConsumerOffset 更新消费者offset
// Author gaoyanlei
// Since 2017/8/25
func (cmp *ClientManageProcessor) updateConsumerOffset(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}

	requestHeader := &header.UpdateConsumerOffsetRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	// 消息轨迹：记录已经消费成功并提交 offset 的消息记录
	if cmp.HasConsumeMessageHook() {
		// 执行hook
		context := &mqtrace.ConsumeMessageContext{}
		context.ConsumerGroup = requestHeader.ConsumerGroup
		context.Topic = requestHeader.Topic
		context.ClientHost = ctx.LocalAddr().String()
		context.Success = true
		context.Status = listener.CONSUME_SUCCESS.String()

		storeHost := cmp.BrokerController.BrokerConfig.BrokerIP1 + "" + cmp.BrokerController.RemotingServer.GetListenPort()

		preOffset := cmp.BrokerController.ConsumerOffsetManager.queryOffset(requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueId)

		// TODO messageIds := cmp.BrokerController.MessageStore.getMessageIds(requestHeader.Topic, requestHeader.QueueId, preOffset, requestHeader.CommitOffset, storeHost)
		messageIds := make(map[string]int64)
		fmt.Println(storeHost)
		fmt.Println(preOffset)

		context.MessageIds = messageIds
		cmp.ExecuteConsumeMessageHookAfter(context)
	}

	cmp.BrokerController.ConsumerOffsetManager.CommitOffset(
		requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueId, requestHeader.CommitOffset)

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// getConsumerListByGroup 通过Group获得消费列表
// Author gaoyanlei
// Since 2017/8/25
func (cmp *ClientManageProcessor) getConsumerListByGroup(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	requestHeader := &header.GetConsumerListByGroupRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	consumerGroupInfo := cmp.BrokerController.ConsumerManager.GetConsumerGroupInfo(requestHeader.ConsumerGroup)
	if consumerGroupInfo != nil {
		clientIds := consumerGroupInfo.GetAllClientId()

		if clientIds != nil && len(clientIds) > 0 {
			body := &header.GetConsumerListByGroupResponseBody{}
			body.ConsumerIdList = clientIds
			response.Body, _ = body.Encode()
			response.Code = code.SUCCESS
			response.Remark = ""
			return response, nil
		}

	} else {
		logger.Warnf("getConsumerGroupInfo failed, %s %s", requestHeader.ConsumerGroup, ctx.RemoteAddr().String())
	}

	response.Code = code.SYSTEM_ERROR
	response.Remark = "no consumer for this group, " + requestHeader.ConsumerGroup
	return response, nil
}

// hasConsumeMessageHook 判断是否有回调函数
// Author rongzhihong
// Since 2017/9/14
func (cmp *ClientManageProcessor) HasConsumeMessageHook() bool {
	return cmp.consumeMessageHookList != nil && len(cmp.consumeMessageHookList) > 0
}

// RegisterConsumeMessageHook 注册回调函数
// Author rongzhihong
// Since 2017/9/14
func (cmp *ClientManageProcessor) RegisterConsumeMessageHook(consumeMessageHookList []mqtrace.ConsumeMessageHook) {
	cmp.consumeMessageHookList = consumeMessageHookList
}

// ExecuteConsumeMessageHookAfter 消费消息后执行的回调函数
// Author rongzhihong
// Since 2017/9/14
func (cmp *ClientManageProcessor) ExecuteConsumeMessageHookAfter(context *mqtrace.ConsumeMessageContext) {
	if cmp.HasConsumeMessageHook() {
		for _, hook := range cmp.consumeMessageHookList {
			hook.ConsumeMessageAfter(context)
		}
	}
}

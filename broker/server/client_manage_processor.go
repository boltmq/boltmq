// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package server

import (
	"github.com/boltmq/boltmq/broker/trace"
	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/boltmq/net/core"
	"github.com/boltmq/common/constant"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/body"
	"github.com/boltmq/common/protocol/head"
	"github.com/boltmq/common/protocol/heartbeat"
	"github.com/boltmq/common/sysflag"
)

type clientManageProcessor struct {
	brokerController       *BrokerController
	consumeMessageHookList []trace.ConsumeMessageHook
}

// newClientManageProcessor 初始化clientManageProcessor
// Author gaoyanlei
// Since 2017/8/9
func newClientManageProcessor(controller *BrokerController) *clientManageProcessor {
	var cmp = new(clientManageProcessor)
	cmp.brokerController = controller
	return cmp
}

func (cmp *clientManageProcessor) ProcessRequest(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	switch request.Code {
	case protocol.HEART_BEAT:
		return cmp.heartBeat(ctx, request)
	case protocol.UNREGISTER_CLIENT:
		return cmp.unregisterClient(ctx, request)
	case protocol.GET_CONSUMER_LIST_BY_GROUP:
		return cmp.getConsumerListByGroup(ctx, request)
	case protocol.QUERY_CONSUMER_OFFSET:
		return cmp.queryConsumerOffset(ctx, request)
	case protocol.UPDATE_CONSUMER_OFFSET:
		return cmp.updateConsumerOffset(ctx, request)
	}
	return nil, nil
}

// heartBeat 心跳服务
// Author gaoyanlei
// Since 2017/8/23
func (cmp *clientManageProcessor) heartBeat(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	// heartbeatDataPlus heartbeatData 层级较多，json难解析，加入heartbeatDataPlus作为中间转化
	heartbeatDataPlus := &heartbeat.HeartbeatDataPlus{}
	heartbeatDataPlus.Decode(request.Body)
	consumerDataSet := heartbeatDataPlus.ConsumerDataSet
	chanInfo := newChannelInfo(ctx, heartbeatDataPlus.ClientID, request.Language, ctx.UniqueSocketAddr().String(), request.Version)
	for _, consumerData := range consumerDataSet {
		subscriptionGroupConfig :=
			cmp.brokerController.subGroupManager.findSubscriptionGroupConfig(consumerData.GroupName)
		if subscriptionGroupConfig != nil {
			topicSysFlag := 0
			if consumerData.UnitMode {
				topicSysFlag = sysflag.TopicBuildSysFlag(false, true)
			}

			newTopic := getRetryTopic(consumerData.GroupName)
			cmp.brokerController.tpConfigManager.createTopicInSendMessageBackMethod(
				newTopic, //
				subscriptionGroupConfig.RetryQueueNums, //
				constant.PERM_WRITE|constant.PERM_READ, topicSysFlag)
		}

		changed := cmp.brokerController.csmManager.registerConsumer(consumerData.GroupName, chanInfo,
			consumerData.ConsumeType, consumerData.MessageModel, consumerData.ConsumeFromWhere, consumerData.SubscriptionDataSet)
		if changed {
			logger.Infof("registerConsumer info changed: RemoteAddr:%s, consumerData:%v.", ctx.RemoteAddr(), consumerData)
		}
	}

	// 注册Producer
	for _, producerData := range heartbeatDataPlus.ProducerDataSet {
		cmp.brokerController.prcManager.registerProducer(producerData.GroupName, chanInfo)
	}

	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// unregisterClient 注销客户端
// Author gaoyanlei
// Since 2017/8/24
func (cmp *clientManageProcessor) unregisterClient(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}

	requestHeader := &head.UnRegisterClientRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("unregisterClient err: %s.", err)
	}

	chanInfo := newChannelInfo(ctx, requestHeader.ClientID, request.Language, ctx.LocalAddr().String(), request.Version)

	// 注销Producer
	{
		group := requestHeader.ProducerGroup
		if group != "" {
			cmp.brokerController.prcManager.unregisterProducer(group, chanInfo)
		}
	}

	// 注销Consumer
	{
		group := requestHeader.ConsumerGroup
		if group != "" {
			cmp.brokerController.csmManager.unregisterConsumer(group, chanInfo)
		}
	}

	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// queryConsumerOffset  查询Consumer的偏移量
// Author rongzhihong
// Since 2017/9/14
func (cmp *clientManageProcessor) queryConsumerOffset(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &head.QueryConsumerOffsetResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	requestHeader := &head.QueryConsumerOffsetRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error("query consumer offset err: %s.", err)
	}

	offset := cmp.brokerController.csmOffsetManager.queryOffset(requestHeader.ConsumerGroup, requestHeader.Topic, int(requestHeader.QueueId))

	// 订阅组存在
	if offset >= 0 {
		responseHeader.Offset = offset
		response.Code = protocol.SUCCESS
		response.Remark = ""
	} else { // 订阅组不存在

		minOffset := cmp.brokerController.messageStore.MinOffsetInQueue(requestHeader.Topic, requestHeader.QueueId)
		isInDisk := cmp.brokerController.messageStore.CheckInDiskByConsumeOffset(requestHeader.Topic, requestHeader.QueueId, 0)

		// 订阅组不存在情况下，如果这个队列的消息最小Offset是0，则表示这个Topic上线时间不长，服务器堆积的数据也不多，那么这个订阅组就从0开始消费。
		// 尤其对于Topic队列数动态扩容时，必须要从0开始消费。
		if minOffset <= 0 && !isInDisk {
			responseHeader.Offset = 0
			response.Code = protocol.SUCCESS
			response.Remark = ""
		} else {
			response.Code = protocol.QUERY_NOT_FOUND
			response.Remark = "Not found, maybe this group consumer boot first"
		}
	}

	return response, nil
}

// updateConsumerOffset 更新消费者offset
// Author gaoyanlei
// Since 2017/8/25
func (cmp *clientManageProcessor) updateConsumerOffset(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}

	requestHeader := &head.UpdateConsumerOffsetRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("update consumer offset err: %s.", err)
	}

	// 消息轨迹：记录已经消费成功并提交 offset 的消息记录
	if cmp.HasConsumeMessageHook() {
		// 执行hook
		context := &trace.ConsumeMessageContext{}
		context.ConsumerGroup = requestHeader.ConsumerGroup
		context.Topic = requestHeader.Topic
		context.ClientHost = ctx.RemoteAddr().String()
		context.Success = true
		context.Status = protocol.CONSUME_SUCCESS.String()

		storeHost := cmp.brokerController.getStoreHost()
		preOffset := cmp.brokerController.csmOffsetManager.queryOffset(requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueId)
		messageIds := cmp.brokerController.messageStore.MessageIds(requestHeader.Topic, int32(requestHeader.QueueId), preOffset, requestHeader.CommitOffset, storeHost)

		context.MessageIds = messageIds
		cmp.ExecuteConsumeMessageHookAfter(context)
	}

	cmp.brokerController.csmOffsetManager.commitOffset(
		requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueId, requestHeader.CommitOffset)

	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// getConsumerListByGroup 通过Group获得消费列表
// Author gaoyanlei
// Since 2017/8/25
func (cmp *clientManageProcessor) getConsumerListByGroup(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	requestHeader := &head.GetConsumersByGroupRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error("get consumer list by group err: %s.", err)
		return nil, err
	}

	consumerGroupInfo := cmp.brokerController.csmManager.getConsumerGroupInfo(requestHeader.ConsumerGroup)
	if consumerGroupInfo != nil {
		clientIds := consumerGroupInfo.getAllClientId()

		if clientIds != nil && len(clientIds) > 0 {
			consumerListBody := &body.GetConsumersByGroupResponse{ConsumerIdList: clientIds}
			response.Body, err = common.Encode(consumerListBody)
			if err != nil {
				return nil, err
			}
			response.Code = protocol.SUCCESS
			response.Remark = ""
			return response, nil
		} else {
			logger.Warnf("get all clientId failed, %s %s.", requestHeader.ConsumerGroup, ctx.RemoteAddr())
		}
	} else {
		logger.Warnf("getConsumerGroupInfo failed, %s %s.", requestHeader.ConsumerGroup, ctx.RemoteAddr())
	}

	response.Code = protocol.SYSTEM_ERROR
	response.Remark = "no consumer for this group, " + requestHeader.ConsumerGroup
	return response, nil
}

// hasConsumeMessageHook 判断是否有回调函数
// Author rongzhihong
// Since 2017/9/14
func (cmp *clientManageProcessor) HasConsumeMessageHook() bool {
	return cmp.consumeMessageHookList != nil && len(cmp.consumeMessageHookList) > 0
}

// RegisterConsumeMessageHook 注册回调函数
// Author rongzhihong
// Since 2017/9/14
func (cmp *clientManageProcessor) RegisterConsumeMessageHook(consumeMessageHookList []trace.ConsumeMessageHook) {
	cmp.consumeMessageHookList = consumeMessageHookList
}

// ExecuteConsumeMessageHookAfter 消费消息后执行的回调函数
// Author rongzhihong
// Since 2017/9/14
func (cmp *clientManageProcessor) ExecuteConsumeMessageHookAfter(context *trace.ConsumeMessageContext) {
	if cmp.HasConsumeMessageHook() {
		for _, hook := range cmp.consumeMessageHookList {
			hook.ConsumeMessageAfter(context)
		}
	}
}

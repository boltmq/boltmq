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
	"fmt"
	"strconv"
	"strings"

	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/net/core"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/base"
	"github.com/boltmq/common/protocol/body"
	"github.com/boltmq/common/protocol/head"
	"github.com/boltmq/common/protocol/stats"
	"github.com/boltmq/common/protocol/subscription"
	set "github.com/deckarep/golang-set"
	"github.com/go-errors/errors"
	"github.com/pquerna/ffjson/ffjson"
)

// adminBrokerProcessor 管理类请求处理
// Author gaoyanlei
// Since 2017/8/23
type adminBrokerProcessor struct {
	brokerController *BrokerController
}

// newAdminBrokerProcessor 初始化
// Author gaoyanlei
// Since 2017/8/23
func newAdminBrokerProcessor(controller *BrokerController) *adminBrokerProcessor {
	admin := &adminBrokerProcessor{
		brokerController: controller,
	}
	return admin
}

// ProcessRequest 请求入口
// Author rongzhihong
// Since 2017/8/23
func (abp *adminBrokerProcessor) ProcessRequest(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	switch request.Code {
	case protocol.UPDATE_AND_CREATE_TOPIC:
		return abp.updateAndCreateTopic(ctx, request) // 更新创建Topic
	case protocol.DELETE_TOPIC_IN_BROKER:
		return abp.deleteTopic(ctx, request) // 删除Topic
	case protocol.GET_ALL_TOPIC_CONFIG:
		return abp.getAllTopicConfig(ctx, request) // 获取所有Topic配置
	case protocol.UPDATE_BROKER_CONFIG:
		return abp.updateBrokerConfig(ctx, request) // TODO: 更新Broker配置,可能存在并发问题
	case protocol.GET_BROKER_CONFIG:
		return abp.getBrokerConfig(ctx, request) // 获取Broker配置
	case protocol.SEARCH_OFFSET_BY_TIMESTAMP:
		return abp.searchOffsetByTimestamp(ctx, request) // 根据时间戳查询Offset
	case protocol.GET_MAX_OFFSET:
		return abp.getMaxOffset(ctx, request) // 获取最大Offset
	case protocol.GET_MIN_OFFSET:
		return abp.getMinOffset(ctx, request) // 获取小Offset
	case protocol.GET_EARLIEST_MSG_STORETIME:
		return abp.getEarliestMsgStoretime(ctx, request) // 查询消息最早存储时间
	case protocol.GET_BROKER_RUNTIME_INFO:
		return abp.getBrokerRuntimeInfo(ctx, request) // 获取Broker运行时信息
	case protocol.LOCK_BATCH_MQ:
		return abp.lockBatchMQ(ctx, request) // 锁队列
	case protocol.UNLOCK_BATCH_MQ:
		return abp.unlockBatchMQ(ctx, request) // 解锁队列
	case protocol.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP:
		return abp.updateAndCreateSubscriptionGroup(ctx, request) // 订阅组配置
	case protocol.GET_ALL_SUBSCRIPTIONGROUP_CONFIG:
		return abp.getAllSubscriptionGroup(ctx, request)
	case protocol.DELETE_SUBSCRIPTIONGROUP:
		return abp.deleteSubscriptionGroup(ctx, request)
	case protocol.GET_TOPIC_STATS_INFO:
		return abp.getTopicStatsInfo(ctx, request) // 统计信息，获取Topic的存储统计信息(minOffset、maxOffset、lastUpdatetime)
	case protocol.GET_CONSUMER_CONNECTION_LIST:
		return abp.getConsumerConnectionList(ctx, request) // Consumer连接管理
	case protocol.GET_PRODUCER_CONNECTION_LIST:
		return abp.getProducerConnectionList(ctx, request) // Producer连接管理
	case protocol.GET_CONSUME_STATS:
		return abp.getConsumeStats(ctx, request) // 查询消费进度，订阅组下的所有Topic
	case protocol.GET_ALL_CONSUMER_OFFSET:
		return abp.getAllConsumerOffset(ctx, request)
	case protocol.GET_ALL_DELAY_OFFSET:
		return abp.getAllDelayOffset(ctx, request) // 定时进度
	case protocol.INVOKE_BROKER_TO_RESET_OFFSET:
		return abp.resetOffset(ctx, request) // 调用客户端重置 offset
	case protocol.INVOKE_BROKER_TO_GET_CONSUMER_STATUS:
		return abp.getConsumerStatus(ctx, request) // 调用客户端订阅消息处理
	case protocol.QUERY_TOPIC_CONSUME_BY_WHO:
		return abp.queryTopicConsumeByWho(ctx, request) // 查询Topic被哪些消费者消费
	case protocol.REGISTER_FILTER_SERVER:
		return abp.registerFilterServer(ctx, request)
	case protocol.QUERY_CONSUME_TIME_SPAN:
		return abp.queryConsumeTimeSpan(ctx, request) // 根据 topic 和 group 获取消息的时间跨度
	case protocol.GET_SYSTEM_TOPIC_LIST_FROM_BROKER:
		return abp.getSystemTopicListFromBroker(ctx, request)
	case protocol.CLEAN_EXPIRED_CONSUMEQUEUE:
		return abp.cleanExpiredConsumeQueue(ctx, request) // 删除失效队列
	case protocol.GET_CONSUMER_RUNNING_INFO:
		return abp.getConsumerRunningInfo(ctx, request)
	case protocol.QUERY_CORRECTION_OFFSET:
		return abp.queryCorrectionOffset(ctx, request) // 查找被修正 offset (转发组件）
	case protocol.CONSUME_MESSAGE_DIRECTLY:
		return abp.consumeMessageDirectly(ctx, request)
	case protocol.CLONE_GROUP_OFFSET:
		return abp.cloneGroupOffset(ctx, request)
	case protocol.VIEW_BROKER_STATS_DATA:
		return abp.ViewBrokerStatsData(ctx, request) // 查看Broker统计信息
	default:

	}
	return nil, nil
}

// updateAndCreateTopic 更新创建TOPIC
// Author rongzhihong
// Since 2017/8/23
func (abp *adminBrokerProcessor) updateAndCreateTopic(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &head.CreateTopicRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("update and create topic err: %s.", err)
		return response, err
	}

	// Topic名字是否与保留字段冲突
	if strings.EqualFold(requestHeader.Topic, abp.brokerController.cfg.Cluster.Name) {
		logger.Infof("the topic[%s] is conflict with system reserved words.", requestHeader.Topic)
		response.Remark = fmt.Sprintf("the topic[%s] is conflict with system reserved words.", requestHeader.Topic)
		return response, nil
	}

	readQueueNums := requestHeader.ReadQueueNums
	writeQueueNums := requestHeader.WriteQueueNums
	brokerPermission := requestHeader.Perm
	topicFilterType := requestHeader.TopicFilterType
	topicConfig := base.NewDefaultTopicConfig(requestHeader.Topic, readQueueNums, writeQueueNums, brokerPermission, topicFilterType)
	if requestHeader.TopicSysFlag != 0 {
		topicConfig.TopicSysFlag = requestHeader.TopicSysFlag
	}
	abp.brokerController.tpConfigManager.updateTopicConfig(topicConfig)
	abp.brokerController.registerBrokerAll(false, true)

	response.Code = protocol.SUCCESS
	response.Remark = ""

	logger.Infof("update and create topic success, topic=%s, cluster name=%s.", requestHeader.Topic, abp.brokerController.cfg.Cluster.Name)
	return response, nil
}

func (abp *adminBrokerProcessor) getMaxOffset(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &head.GetMaxOffsetResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	requestHeader := head.NewGetMaxOffsetRequestHeader()
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, err
	}

	offset := abp.brokerController.messageStore.MaxOffsetInQueue(requestHeader.Topic, int32(requestHeader.QueueId))

	responseHeader.Offset = offset
	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}
func (abp *adminBrokerProcessor) deleteTopic(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	requestHeader := &head.DeleteTopicRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, err
	}

	abp.brokerController.tpConfigManager.deleteTopicConfig(requestHeader.Topic)
	abp.brokerController.tasks.startDeleteTopicTask()

	logger.Infof("delete topic called by %s.", ctx.LocalAddr())
	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// getAllTopicConfig 获得Topic配置信息
// Author rongzhihong
// Since 2017/9/19
func (adp *adminBrokerProcessor) getAllTopicConfig(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &head.GetAllTopicConfigResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	content := adp.brokerController.tpConfigManager.encode(false)
	logger.Infof("all topic config is %s.", content)

	if content != "" && len(content) > 0 {
		response.Body = []byte(content)
		response.Code = protocol.SUCCESS
		response.Remark = ""
		return response, nil
	}

	logger.Errorf("no topic in this broker, client: %s.", ctx.RemoteAddr())
	response.Code = protocol.SYSTEM_ERROR
	response.Remark = "no topic in this broker"
	return response, nil
}

// updateBrokerConfig 更新Broker服务器端的BrokerConfig, MessageStoreConfig信息
// Author rongzhihong
// Since 2017/9/19
func (adp *adminBrokerProcessor) updateBrokerConfig(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	logger.Infof("update broker config called by %s.", parseChannelRemoteAddr(ctx))

	response := protocol.CreateDefaultResponseCommand()
	if request.Body == nil {
		logger.Error("update broker config, content is nil.")
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = "content is nil"
		return response, nil
	}

	logger.Infof("update broker config, new config: %s, client: %s.", string(request.Body), ctx.RemoteAddr())
	adp.brokerController.updateAllConfig(request.Body)
	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// getBrokerConfig 获得Broker配置信息
// Author rongzhihong
// Since 2017/9/19
func (adp *adminBrokerProcessor) getBrokerConfig(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &head.GetBrokerConfigResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	content := adp.brokerController.readConfig()
	if content != "" && len(content) > 0 {
		response.Body = []byte(content)
	}

	responseHeader.Version = adp.brokerController.dataVersion.Json()
	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// searchOffsetByTimestamp 根据时间查询偏移量
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) searchOffsetByTimestamp(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &head.SearchOffsetResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	requestHeader := &head.SearchOffsetRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	offset := abp.brokerController.messageStore.OffsetInQueueByTime(requestHeader.Topic, requestHeader.QueueId, requestHeader.Timestamp)
	responseHeader.Offset = offset
	response.Code = protocol.SUCCESS
	response.Remark = ""

	return response, nil
}

// getMinOffset 获得最小偏移量
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) getMinOffset(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &head.GetMinOffsetResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	requestHeader := &head.GetMinOffsetRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	offset := abp.brokerController.messageStore.MinOffsetInQueue(requestHeader.Topic, requestHeader.QueueId)
	responseHeader.Offset = offset
	response.Code = protocol.SUCCESS
	response.Remark = ""

	return response, nil
}

// getEarliestMsgStoretime 获得最早消息存储时间
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) getEarliestMsgStoretime(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &head.GetEarliestMsgStoretimeResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	requestHeader := &head.GetEarliestMsgStoretimeRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	timestamp := abp.brokerController.messageStore.EarliestMessageTime(requestHeader.Topic, requestHeader.QueueId)
	responseHeader.Timestamp = timestamp
	response.Code = protocol.SUCCESS
	response.Remark = ""

	return response, nil
}

// getBrokerRuntimeInfo 获取Broker运行时信息
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) getBrokerRuntimeInfo(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	runtimeInfo := abp.prepareRuntimeInfo()

	kvTable := &protocol.KVTable{}
	kvTable.Table = runtimeInfo
	content, err := common.Encode(kvTable)
	if err != nil {
		return nil, err
	}

	response.Body = content
	response.Code = protocol.SUCCESS
	response.Remark = ""

	return response, nil
}

// lockBatchMQ 锁队列
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) lockBatchMQ(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	requestBody := body.NewLockBatchRequest()
	err := common.Decode(request.Body, requestBody)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	lockOKMQSet := abp.brokerController.rblManager.tryLockBatch(requestBody.ConsumerGroup,
		requestBody.MQSet, requestBody.ClientId)

	responseBody := body.NewLockBatchResponse()
	responseBody.LockOKMQSet = lockOKMQSet

	content, err := common.Encode(responseBody)
	if err != nil {
		return nil, err
	}

	response.Body = content
	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// lockBatchMQ 解锁队列
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) unlockBatchMQ(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	requestBody := body.NewUnlockBatchRequest()
	err := common.Decode(request.Body, requestBody)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	abp.brokerController.rblManager.unlockBatch(requestBody.ConsumerGroup, requestBody.MQSet, requestBody.ClientId)

	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// prepareRuntimeInfo 组装运行中的Broker的信息
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) prepareRuntimeInfo() map[string]string {
	runtimeInfo := abp.brokerController.messageStore.RuntimeInfo()
	runtimeInfo["brokerVersionDesc"] = common.Version
	runtimeInfo["brokerVersion"] = fmt.Sprintf("%d", common.Version)

	runtimeInfo["msgPutTotalYesterdayMorning"] = fmt.Sprintf("%d", abp.brokerController.brokerStatsRelatedStore.GetMsgPutTotalYesterdayMorning())
	runtimeInfo["msgPutTotalTodayMorning"] = fmt.Sprintf("%d", abp.brokerController.brokerStatsRelatedStore.GetMsgPutTotalTodayMorning())
	runtimeInfo["msgPutTotalTodayNow"] = fmt.Sprintf("%d", abp.brokerController.brokerStatsRelatedStore.GetMsgPutTotalTodayNow())

	runtimeInfo["msgGetTotalYesterdayMorning"] = fmt.Sprintf("%d", abp.brokerController.brokerStatsRelatedStore.GetMsgGetTotalYesterdayMorning())
	runtimeInfo["msgGetTotalTodayMorning"] = fmt.Sprintf("%d", abp.brokerController.brokerStatsRelatedStore.GetMsgGetTotalTodayMorning())
	runtimeInfo["msgGetTotalTodayNow"] = fmt.Sprintf("%d", abp.brokerController.brokerStatsRelatedStore.GetMsgGetTotalTodayNow())

	runtimeInfo["sendThreadPoolQueueCapacity"] = fmt.Sprintf("%d", abp.brokerController.cfg.Broker.SendThreadPoolQueueCapacity)

	return runtimeInfo
}

// updateAndCreateSubscriptionGroup 更新或者创建消费分组
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) updateAndCreateSubscriptionGroup(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	logger.Infof("update and create subscription group called by %s.", parseChannelRemoteAddr(ctx))
	config := &subscription.SubscriptionGroupConfig{}
	err := common.Decode(request.Body, config)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	if config != nil {
		abp.brokerController.subGroupManager.updateSubscriptionGroupConfig(config)
	}

	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// getAllSubscriptionGroup 获得所有消费分组
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) getAllSubscriptionGroup(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	content := abp.brokerController.subGroupManager.encode(false)

	if content != "" && len(content) > 0 {
		response.Body = []byte(content)
		response.Code = protocol.SUCCESS
		response.Remark = ""
		return response, nil
	}

	logger.Errorf("No subscription group in this broker, client: %s.", ctx.RemoteAddr())
	response.Code = protocol.SYSTEM_ERROR
	response.Remark = "No subscription group in this broker"
	return response, nil
}

// deleteSubscriptionGroup 删除消费分组
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) deleteSubscriptionGroup(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	requestHeader := &head.DeleteSubscriptionGroupRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	logger.Infof("delete subscription group called by %s.", parseChannelRemoteAddr(ctx))
	abp.brokerController.subGroupManager.deleteSubscriptionGroupConfig(requestHeader.GroupName)

	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// getTopicStatsInfo 获取Topic的存储统计信息(minOffset、maxOffset、lastUpdateTime)
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) getTopicStatsInfo(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &head.GetTopicStatsInfoRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	topic := requestHeader.Topic
	topicConfig := abp.brokerController.tpConfigManager.selectTopicConfig(topic)
	if topicConfig == nil {
		response.Code = protocol.TOPIC_NOT_EXIST
		response.Remark = fmt.Sprintf("topic[%s] not exist", topic)
		return response, nil
	}

	topicStatsTable := stats.NewTopicStatsTablePlus()
	var writeQueueNums = int(topicConfig.WriteQueueNums)
	for i := 0; i < writeQueueNums; i++ {
		mq := &message.MessageQueue{}
		mq.Topic = topic
		mq.BrokerName = abp.brokerController.cfg.Cluster.BrokerName
		mq.QueueId = i

		topicOffset := stats.NewTopicOffset()
		min := abp.brokerController.messageStore.MinOffsetInQueue(topic, int32(i))
		if min < 0 {
			min = 0
		}

		max := abp.brokerController.messageStore.MaxOffsetInQueue(topic, int32(i))
		if max < 0 {
			max = 0
		}

		timestamp := int64(0)
		if max > 0 {
			timestamp = abp.brokerController.messageStore.MessageStoreTimeStamp(topic, int32(i), max-1)
		}

		topicOffset.MinOffset = min
		topicOffset.MaxOffset = max
		topicOffset.LastUpdateTimestamp = timestamp
		mqKey := mq.Key()
		topicStatsTable.OffsetTable[mqKey] = topicOffset
	}

	content, err := ffjson.Marshal(topicStatsTable)
	if err != nil {
		return nil, err
	}

	response.Code = protocol.SUCCESS
	response.Body = content
	response.Remark = ""

	return response, nil
}

// getConsumerConnectionList 获得消费者连接信息
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) getConsumerConnectionList(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	requestHeader := &head.GetConsumerConnectionsRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	cgi := abp.brokerController.csmManager.getConsumerGroupInfo(requestHeader.ConsumerGroup)
	if cgi != nil {
		bodydata := body.NewConsumerConnectionPlus()
		bodydata.ConsumeFromWhere = cgi.consumeFromWhere
		bodydata.ConsumeType = cgi.consumeType
		bodydata.MessageModel = cgi.msgModel
		subscriptionMap := cgi.subscriptionTableToMap()
		bodydata.SubscriptionTable = subscriptionMap
		iterator := cgi.connTable.Iterator()
		for iterator.HasNext() {
			_, value, _ := iterator.Next()
			if info, ok := value.(*channelInfo); ok {
				connection := &body.Connection{}
				connection.ClientId = info.clientId
				connection.Language = info.langCode
				connection.Version = info.version
				connection.ClientAddr = parseChannelRemoteAddr(info.ctx)
				bodydata.ConnectionSet = append(bodydata.ConnectionSet, connection)
			}
		}

		content, err := common.Encode(bodydata)
		if err != nil {
			return nil, err
		}

		response.Body = content
		response.Code = protocol.SUCCESS
		response.Remark = ""
		return response, nil
	}

	response.Code = protocol.CONSUMER_NOT_ONLINE
	response.Remark = fmt.Sprintf("the consumer group[%s] not online", requestHeader.ConsumerGroup)
	return response, nil
}

// getProducerConnectionList 获得生产者连接信息
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) getProducerConnectionList(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	requestHeader := &head.GetProducerConnectionsRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	channelInfoHashMap := abp.brokerController.prcManager.getGroupChannelTable().Get(requestHeader.ProducerGroup)
	if channelInfoHashMap != nil {
		bodydata := body.NewProducerConnection()
		for _, info := range channelInfoHashMap {
			connection := &body.Connection{}
			connection.ClientId = info.clientId
			connection.Language = info.langCode
			connection.Version = info.version
			connection.ClientAddr = parseChannelRemoteAddr(info.ctx)

			bodydata.ConnectionSet.Add(connection)
		}

		content, err := common.Encode(bodydata)
		if err != nil {
			return nil, err
		}

		response.Body = content
		response.Code = protocol.SUCCESS
		response.Remark = ""
		return response, nil
	}

	response.Code = protocol.SYSTEM_ERROR
	response.Remark = fmt.Sprintf("the producer group[%s] not exist", requestHeader.ProducerGroup)
	return response, nil
}

// getConsumeStats 获得消费者统计信息
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) getConsumeStats(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	requestHeader := &head.GetConsumerStatsRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	consumeStats := stats.NewConsumeStatsPlus()

	topics := set.NewSet()
	if common.IsBlank(requestHeader.Topic) {
		topics = abp.brokerController.csmOffsetManager.whichTopicByConsumer(requestHeader.ConsumerGroup)
	} else {
		topics.Add(requestHeader.Topic)
	}

	for topic := range topics.Iterator().C {

		if topic, ok := topic.(string); ok {

			topicConfig := abp.brokerController.tpConfigManager.selectTopicConfig(topic)
			if nil == topicConfig {
				logger.Warnf("consumeStats, topic config not exist, %s.", topic)
				continue
			}

			// Consumer不在线的时候，也允许查询消费进度
			{
				findSubscriptionData := abp.brokerController.csmManager.findSubscriptionData(requestHeader.ConsumerGroup, topic)
				// 如果Consumer在线，而且这个topic没有被订阅，那么就跳过
				if nil == findSubscriptionData && abp.brokerController.csmManager.findSubscriptionDataCount(
					requestHeader.ConsumerGroup) > 0 {
					logger.Warnf("consumeStats, the consumer group[%s], topic[%s] not exist.",
						requestHeader.ConsumerGroup, topic)
					continue
				}
			}

			var writeQueueNums int = int(topicConfig.WriteQueueNums)
			for i := 0; i < writeQueueNums; i++ {
				mq := &message.MessageQueue{}
				mq.Topic = topic
				mq.BrokerName = abp.brokerController.cfg.Cluster.BrokerName
				mq.QueueId = i

				offsetWrapper := &stats.OffsetWrapper{}
				brokerOffset := abp.brokerController.messageStore.MaxOffsetInQueue(topic, int32(i))
				if brokerOffset < 0 {
					brokerOffset = 0
				}

				consumerOffset := abp.brokerController.csmOffsetManager.queryOffset(requestHeader.ConsumerGroup, topic, i)
				if consumerOffset < 0 {
					consumerOffset = 0
				}

				offsetWrapper.BrokerOffset = brokerOffset
				offsetWrapper.ConsumerOffset = consumerOffset

				// 查询消费者最后一条消息对应的时间戳
				timeOffset := consumerOffset - 1
				if timeOffset >= 0 {
					lastTimestamp := abp.brokerController.messageStore.MessageStoreTimeStamp(topic, int32(i), timeOffset)
					if lastTimestamp > 0 {
						offsetWrapper.LastTimestamp = lastTimestamp
					}
				}

				mqKey := fmt.Sprintf("%s@%s@%d", mq.Topic, mq.BrokerName, mq.QueueId)
				consumeStats.OffsetTable[mqKey] = offsetWrapper
			}

			consumeTps := abp.brokerController.brokerStats.TpsGroupGetNums(requestHeader.ConsumerGroup, topic)
			consumeStats.ConsumeTps += consumeTps
		}
	}

	consumeTpsStr := fmt.Sprintf("%0.2f", consumeStats.ConsumeTps)
	consumeTpsMath, _ := strconv.ParseFloat(consumeTpsStr, 64)
	consumeStats.ConsumeTps = consumeTpsMath

	content, err := common.Encode(consumeStats)
	if err != nil {
		return nil, err
	}

	response.Body = content
	response.Code = protocol.SUCCESS
	response.Remark = ""

	return response, nil
}

// getAllConsumerOffset 所有消费者的偏移量
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) getAllConsumerOffset(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	content := abp.brokerController.csmOffsetManager.encode(false)
	if content != "" && len(content) > 0 {
		response.Body = []byte(content)
	} else {
		logger.Errorf("No consumer offset in this broker, client: %s.", ctx.RemoteAddr())
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = "No consumer offset in this broker"
		return response, nil
	}

	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// getAllDelayOffset 所有消费者的定时偏移量
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) getAllDelayOffset(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	content := abp.brokerController.messageStore.EncodeScheduleMsg()
	if len(content) > 0 {
		response.Body = []byte(content)
	} else {
		logger.Errorf("No delay offset in this broker, client: %s.", ctx.RemoteAddr())
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = "No delay offset in this broker"
		return response, nil
	}

	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// resetOffset 所有消费者的定时偏移量
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) resetOffset(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	requestHeader := &head.ResetOffsetRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	logger.Infof("[reset-offset] reset offset started by %s. topic=%s, group=%s, timestamp=%s, isForce=%t.",
		parseChannelRemoteAddr(ctx), requestHeader.Topic, requestHeader.Group, requestHeader.Timestamp, requestHeader.IsForce)

	return abp.brokerController.b2Client.resetOffset(requestHeader.Topic, requestHeader.Group, requestHeader.Timestamp, requestHeader.IsForce), nil
}

// getConsumerStatus Broker主动获取Consumer端的消息情况
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) getConsumerStatus(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	requestHeader := &head.GetConsumerStatusRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	logger.Infof("[get-consumer-status] get consumer status by %s. topic=%s, group=%s",
		parseChannelRemoteAddr(ctx), requestHeader.Topic, requestHeader.Group)

	return abp.brokerController.b2Client.getConsumeStatus(requestHeader.Topic, requestHeader.Group, requestHeader.ClientAddr), nil
}

// queryTopicConsumeByWho 查询Topic被哪些消费者消费
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) queryTopicConsumeByWho(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	requestHeader := &head.QueryTopicConsumeByWhoRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	// 从订阅关系查询topic被谁消费，只查询在线
	groups := abp.brokerController.csmManager.queryTopicConsumeByWho(requestHeader.Topic)

	// 从Offset持久化查询topic被谁消费，离线和在线都会查询
	groupInOffset := abp.brokerController.csmOffsetManager.whichGroupByTopic(requestHeader.Topic)
	if groupInOffset != nil {
		groups = groups.Union(groupInOffset)
	}

	groupList := body.NewGroupList()
	groupList.GroupList = groups
	content, err := common.Encode(groupList)
	if err != nil {
		return nil, err
	}

	response.Body = content
	response.Code = protocol.SUCCESS
	response.Remark = ""

	return response, nil
}

// registerFilterServer 注册过滤器
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) registerFilterServer(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &head.RegisterFilterServerResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	requestHeader := &head.RegisterFilterServerRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	abp.brokerController.filterSrvManager.registerFilterServer(ctx, requestHeader.FilterServerAddr)

	responseHeader.BrokerId = abp.brokerController.cfg.Cluster.BrokerId
	responseHeader.BrokerName = abp.brokerController.cfg.Cluster.BrokerName

	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// queryConsumeTimeSpan 根据 topic 和 group 获取消息的相关时间(最早存储时间、最晚存储时间、最新消费时间)
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) queryConsumeTimeSpan(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	requestHeader := &head.QueryConsumerTimeSpanRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	topic := requestHeader.Topic
	topicConfig := abp.brokerController.tpConfigManager.selectTopicConfig(topic)
	if nil == topicConfig {
		response.Code = protocol.TOPIC_NOT_EXIST
		response.Remark = fmt.Sprintf("topic[%s] not exist", topic)
		return response, nil
	}

	timeSpanSet := set.NewSet()
	var writeQueueNums int = int(topicConfig.WriteQueueNums)
	for i := 0; i < writeQueueNums; i++ {
		timeSpan := &body.QueueTimeSpan{}
		mq := &message.MessageQueue{}
		mq.Topic = topic
		mq.BrokerName = abp.brokerController.cfg.Cluster.BrokerName
		mq.QueueId = i
		timeSpan.MessageQueue = mq

		minTime := abp.brokerController.messageStore.EarliestMessageTime(topic, int32(i))
		timeSpan.MinTimeStamp = minTime

		max := abp.brokerController.messageStore.MaxOffsetInQueue(topic, int32(i))
		maxTime := abp.brokerController.messageStore.MessageStoreTimeStamp(topic, int32(i), (max - 1))

		timeSpan.MaxTimeStamp = maxTime

		var consumeTime int64
		consumerOffset := abp.brokerController.csmOffsetManager.queryOffset(requestHeader.Group, topic, i)
		if consumerOffset > 0 {
			consumeTime = abp.brokerController.messageStore.MessageStoreTimeStamp(topic, int32(i), consumerOffset)
		} else {
			consumeTime = minTime
		}
		timeSpan.ConsumeTimeStamp = consumeTime
		timeSpanSet.Add(timeSpan)
	}

	queryConsumeTimeSpanBody := body.NewQueryConsumeTimeSpan()
	queryConsumeTimeSpanBody.ConsumeTimeSpanSet = timeSpanSet
	content, err := common.Encode(queryConsumeTimeSpanBody)
	if err != nil {
		return nil, err
	}

	response.Body = content
	response.Code = protocol.SUCCESS
	response.Remark = ""

	return response, nil
}

// getSystemTopicListFromBroker 从Broker获取系统Topic列表
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) getSystemTopicListFromBroker(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	topics := abp.brokerController.tpConfigManager.systemTopicList

	topicList := body.NewTopicList()
	topicList.TopicList = topics
	content, err := common.Encode(topicList)
	if err != nil {
		return nil, err
	}

	response.Body = content
	response.Code = protocol.SUCCESS
	response.Remark = ""

	return response, nil
}

// cleanExpiredConsumeQueue 删除失效消费队列
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) cleanExpiredConsumeQueue(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	//logger.Warn("invoke cleanExpiredConsumeQueue start.")
	abp.brokerController.messageStore.CleanExpiredConsumerQueue()

	response := protocol.CreateDefaultResponseCommand()
	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// getConsumerRunningInfo 调用Consumer，获取Consumer内存数据结构，为监控以及定位问题
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) getConsumerRunningInfo(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	requestHeader := &head.GetConsumerRunningInfoRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	return abp.callConsumer(protocol.GET_CONSUMER_RUNNING_INFO, request, requestHeader.ConsumerGroup, requestHeader.ClientId)
}

// callConsumer call Consumer
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) callConsumer(requestCode int32, request *protocol.RemotingCommand, consumerGroup, clientId string) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	chanInfo := abp.brokerController.csmManager.findChannel(consumerGroup, clientId)
	if nil == chanInfo {
		response.Code = protocol.SYSTEM_ERROR
		format := "The Consumer <%s> <%s> not online"
		response.Remark = fmt.Sprintf(format, consumerGroup, clientId)
		return response, nil
	}

	newRequest := protocol.CreateRequestCommand(requestCode)
	newRequest.ExtFields = request.ExtFields
	newRequest.Body = request.Body

	consumerResponse, err := abp.brokerController.b2Client.callClient(chanInfo.ctx, newRequest)
	if err != nil {
		response.Code = protocol.SYSTEM_ERROR
		format := "invoke consumer <%s> <%s> Exception: %s"
		response.Remark = fmt.Sprintf(format, consumerGroup, clientId, err.Error())
		return response, nil
	}
	return consumerResponse, nil
}

// queryCorrectionOffset 查找被修正 offset (转发组件）
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) queryCorrectionOffset(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	requestHeader := &head.QueryCorrectionOffsetRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	correctionOffset := abp.brokerController.csmOffsetManager.queryMinOffsetInAllGroup(requestHeader.Topic, requestHeader.FilterGroups)

	compareOffset := abp.brokerController.csmOffsetManager.queryOffsetByGroupAndTopic(requestHeader.CompareGroup, requestHeader.Topic)

	if compareOffset != nil && len(compareOffset) > 0 {
		for queueId, v := range compareOffset {
			if correctionOffset[queueId] > v {
				correctionOffset[queueId] = MAX_VALUE
			} else {
				correctionOffset[queueId] = v
			}
		}
	}

	correctionBody := body.NewQueryCorrectionOffset()
	correctionBody.CorrectionOffsets = correctionOffset
	content, err := common.Encode(correctionBody)
	if err != nil {
		return nil, err
	}

	response.Body = content
	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// consumeMessageDirectly 直接让客户端消费消息
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) consumeMessageDirectly(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	requestHeader := &head.ConsumeMessageDirectlyResultRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	request.ExtFields["brokerName"] = abp.brokerController.cfg.Cluster.BrokerName
	messageId, err := message.DecodeMessageId(requestHeader.MsgId)
	if err != nil {
		//return nil, nil
		return nil, errors.Wrap(err, 0)
	}

	bufferResult := abp.brokerController.messageStore.SelectOneMessageByOffset(int64(messageId.Offset))
	if nil != bufferResult {
		length := bufferResult.Size()
		readContent := make([]byte, length)
		bufferResult.Buffer().Read(readContent)
		request.Body = readContent
		bufferResult.Release()
	}

	return abp.callConsumer(protocol.CONSUME_MESSAGE_DIRECTLY, request, requestHeader.ConsumerGroup, requestHeader.ClientId)
}

// cloneGroupOffset 克隆cloneGroupOffset
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) cloneGroupOffset(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	requestHeader := &head.CloneGroupOffsetRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	topics := set.NewSet()

	if common.IsBlank(requestHeader.Topic) {
		topics = abp.brokerController.csmOffsetManager.whichTopicByConsumer(requestHeader.SrcGroup)
	} else {
		topics.Add(requestHeader.Topic)
	}

	for item := range topics.Iterator().C {
		if topic, ok := item.(string); ok {
			topicConfig := abp.brokerController.tpConfigManager.selectTopicConfig(topic)
			if nil == topicConfig {
				logger.Warnf("[cloneGroupOffset], topic config not exist, %s.", topic)
				continue
			}

			// Consumer不在线的时候，也允许查询消费进度
			if !requestHeader.Offline {
				// 如果Consumer在线，而且这个topic没有被订阅，那么就跳过
				findSubscriptionData := abp.brokerController.csmManager.findSubscriptionData(requestHeader.SrcGroup, topic)
				subscriptionDataCount := abp.brokerController.csmManager.findSubscriptionDataCount(requestHeader.SrcGroup)
				if nil == findSubscriptionData && subscriptionDataCount > 0 {
					logger.Warnf("[cloneGroupOffset], the consumer group[%s], topic[%s] not exist.",
						requestHeader.SrcGroup, topic)
					continue
				}
			}

			abp.brokerController.csmOffsetManager.cloneOffset(requestHeader.SrcGroup, requestHeader.DestGroup, requestHeader.Topic)
		}
	}

	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// ViewBrokerStatsData 查看Broker统计信息
// Author rongzhihong
// Since 2017/9/19
func (abp *adminBrokerProcessor) ViewBrokerStatsData(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	requestHeader := &head.ViewBrokerStatsDataRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	statsItem := abp.brokerController.messageStore.BrokerStats().GetStatsItem(requestHeader.StatsName, requestHeader.StatsKey)
	if nil == statsItem {
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = fmt.Sprintf("The stats <%s> <%s> not exist", requestHeader.StatsName, requestHeader.StatsKey)
		return response, nil
	}

	brokerStatsData := body.NewBrokerStatsData()
	// 分钟
	{
		item := &body.BrokerStatsItem{}
		ss := statsItem.GetStatsDataInMinute()
		item.Sum = ss.Sum
		item.Tps = ss.Tps
		item.Avgpt = ss.Avgpt
		brokerStatsData.StatsMinute = item
	}

	// 小时
	{
		item := &body.BrokerStatsItem{}
		ss := statsItem.GetStatsDataInHour()
		item.Sum = ss.Sum
		item.Tps = ss.Tps
		item.Avgpt = ss.Avgpt
		brokerStatsData.StatsHour = item
	}

	// 天
	{
		item := &body.BrokerStatsItem{}
		ss := statsItem.GetStatsDataInDay()
		item.Sum = ss.Sum
		item.Tps = ss.Tps
		item.Avgpt = ss.Avgpt
		brokerStatsData.StatsDay = item
	}

	content, err := common.Encode(brokerStatsData)
	if err != nil {
		return nil, err
	}

	response.Body = content
	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

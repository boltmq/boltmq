package stgbroker

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker/client"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/admin"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/mqversion"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	protocol2 "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/filtersrv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/subscription"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/remotingUtil"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	set "github.com/deckarep/golang-set"
	"strings"
)

// AdminBrokerProcessor 管理类请求处理
// Author gaoyanlei
// Since 2017/8/23
type AdminBrokerProcessor struct {
	BrokerController *BrokerController
}

// NewAdminBrokerProcessor 初始化
// Author gaoyanlei
// Since 2017/8/23
func NewAdminBrokerProcessor(brokerController *BrokerController) *AdminBrokerProcessor {
	var adminBrokerProcessor = new(AdminBrokerProcessor)
	adminBrokerProcessor.BrokerController = brokerController
	return adminBrokerProcessor
}

// ProcessRequest 请求入口
// Author rongzhihong
// Since 2017/8/23
func (abp *AdminBrokerProcessor) ProcessRequest(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	switch request.Code {
	// 更新创建Topic
	case protocol2.UPDATE_AND_CREATE_TOPIC:
		return abp.updateAndCreateTopic(ctx, request)

		// 删除Topic
	case protocol2.DELETE_TOPIC_IN_BROKER:
		return abp.deleteTopic(ctx, request)

		// 获取Topic配置
	case protocol2.GET_ALL_TOPIC_CONFIG:
		return abp.getAllTopicConfig(ctx, request)

		// 更新Broker配置 TODO 可能存在并发问题
	case protocol2.UPDATE_BROKER_CONFIG:
		return abp.updateBrokerConfig(ctx, request)

		// 获取Broker配置
	case protocol2.GET_BROKER_CONFIG:
		return abp.getBrokerConfig(ctx, request)

		// 根据时间查询Offset
	case protocol2.SEARCH_OFFSET_BY_TIMESTAMP:
		return abp.searchOffsetByTimestamp(ctx, request)
	case protocol2.GET_MAX_OFFSET:
		return abp.getMaxOffset(ctx, request)
	case protocol2.GET_MIN_OFFSET:
		return abp.getMinOffset(ctx, request)
	case protocol2.GET_EARLIEST_MSG_STORETIME:
		return abp.getEarliestMsgStoretime(ctx, request)

		// 获取Broker运行时信息
	case protocol2.GET_BROKER_RUNTIME_INFO:
		return abp.getBrokerRuntimeInfo(ctx, request)

		// 锁队列与解锁队列
	case protocol2.LOCK_BATCH_MQ:
		return abp.lockBatchMQ(ctx, request)
	case protocol2.UNLOCK_BATCH_MQ:
		return abp.unlockBatchMQ(ctx, request)

		// 订阅组配置
	case protocol2.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP:
		return abp.updateAndCreateSubscriptionGroup(ctx, request)
	case protocol2.GET_ALL_SUBSCRIPTIONGROUP_CONFIG:
		return abp.getAllSubscriptionGroup(ctx, request)
	case protocol2.DELETE_SUBSCRIPTIONGROUP:
		return abp.deleteSubscriptionGroup(ctx, request)

		// 统计信息，获取Topic统计信息
	case protocol2.GET_TOPIC_STATS_INFO:
		return abp.getTopicStatsInfo(ctx, request)

		// Consumer连接管理
	case protocol2.GET_CONSUMER_CONNECTION_LIST:
		return abp.getConsumerConnectionList(ctx, request)

		// Producer连接管理
	case protocol2.GET_PRODUCER_CONNECTION_LIST:
		return abp.getProducerConnectionList(ctx, request)

		// 查询消费进度，订阅组下的所有Topic
	case protocol2.GET_CONSUME_STATS:
		return abp.getConsumeStats(ctx, request)
	case protocol2.GET_ALL_CONSUMER_OFFSET:
		return abp.getAllConsumerOffset(ctx, request)

		// 定时进度
	case protocol2.GET_ALL_DELAY_OFFSET:
		return abp.getAllDelayOffset(ctx, request)

		// 调用客户端重置 offset
	case protocol2.INVOKE_BROKER_TO_RESET_OFFSET:
		return abp.resetOffset(ctx, request)

		// 调用客户端订阅消息处理
	case protocol2.INVOKE_BROKER_TO_GET_CONSUMER_STATUS:
		return abp.getConsumerStatus(ctx, request)

		// 查询Topic被哪些消费者消费
	case protocol2.QUERY_TOPIC_CONSUME_BY_WHO:
		return abp.queryTopicConsumeByWho(ctx, request)

	case protocol2.REGISTER_FILTER_SERVER:
		return abp.registerFilterServer(ctx, request)

		// 根据 topic 和 group 获取消息的时间跨度
	case protocol2.QUERY_CONSUME_TIME_SPAN:
		return abp.queryConsumeTimeSpan(ctx, request)
	case protocol2.GET_SYSTEM_TOPIC_LIST_FROM_BROKER:
		return abp.getSystemTopicListFromBroker(ctx, request)

		// 删除失效队列
	case protocol2.CLEAN_EXPIRED_CONSUMEQUEUE:
		return abp.cleanExpiredConsumeQueue(ctx, request)

	case protocol2.GET_CONSUMER_RUNNING_INFO:
		return abp.getConsumerRunningInfo(ctx, request)

		// 查找被修正 offset (转发组件）
	case protocol2.QUERY_CORRECTION_OFFSET:
		return abp.queryCorrectionOffset(ctx, request)

	case protocol2.CONSUME_MESSAGE_DIRECTLY:
		return abp.consumeMessageDirectly(ctx, request)
	case protocol2.CLONE_GROUP_OFFSET:
		return abp.cloneGroupOffset(ctx, request)

		// 查看Broker统计信息
	case protocol2.VIEW_BROKER_STATS_DATA:
		return abp.ViewBrokerStatsData(ctx, request)
	}
	return nil, nil
}

// updateAndCreateTopic 更新创建TOPIC
// Author rongzhihong
// Since 2017/8/23
func (abp *AdminBrokerProcessor) updateAndCreateTopic(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	requestHeader := &header.CreateTopicRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	// Topic名字是否与保留字段冲突
	if strings.EqualFold(requestHeader.Topic, abp.BrokerController.BrokerConfig.BrokerClusterName) {
		errorMsg :=
			"the topic[" + requestHeader.Topic + "] is conflict with system reserved words."
		logger.Warn(errorMsg)
		response.Code = code.SYSTEM_ERROR
		response.Remark = errorMsg
		return response, nil
	}

	topicConfig := &stgcommon.TopicConfig{
		TopicName:       requestHeader.Topic,
		ReadQueueNums:   requestHeader.ReadQueueNums,
		WriteQueueNums:  requestHeader.WriteQueueNums,
		TopicFilterType: requestHeader.TopicFilterType,
		Perm:            requestHeader.Perm,
	}
	if requestHeader.TopicSysFlag != 0 {
		topicConfig.TopicSysFlag = requestHeader.TopicSysFlag
	}
	abp.BrokerController.TopicConfigManager.UpdateTopicConfig(topicConfig)
	abp.BrokerController.RegisterBrokerAll(false, true)

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

func (abp *AdminBrokerProcessor) getMaxOffset(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &header.GetMaxOffsetResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	requestHeader := header.NewGetMaxOffsetRequestHeader()
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	offset := abp.BrokerController.MessageStore.GetMaxOffsetInQueue(requestHeader.Topic, int32(requestHeader.QueueId))

	responseHeader.Offset = offset
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}
func (abp *AdminBrokerProcessor) deleteTopic(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	requestHeader := &header.DeleteTopicRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	abp.BrokerController.TopicConfigManager.DeleteTopicConfig(requestHeader.Topic)
	abp.BrokerController.addDeleteTopicTask()

	logger.Infof("deleteTopic called by %v", ctx.LocalAddr().String())
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// getAllTopicConfig 获得Topic配置信息
// Author rongzhihong
// Since 2017/9/19
func (adp *AdminBrokerProcessor) getAllTopicConfig(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &header.GetAllTopicConfigResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	content := adp.BrokerController.TopicConfigManager.Encode(false)
	logger.Infof("AllTopicConfig: %s", content)

	if content != "" && len(content) > 0 {
		response.Body = []byte(content)
		response.Code = code.SUCCESS
		response.Remark = ""
		return response, nil

	} else {
		logger.Errorf("No topic in this broker, client: %s", ctx.RemoteAddr().String())
		response.Code = code.SYSTEM_ERROR
		response.Remark = "No topic in this broker"
		return response, nil
	}
}

// updateBrokerConfig 更新Broker服务器端的BrokerConfig, MessageStoreConfig信息
// Author rongzhihong
// Since 2017/9/19
func (adp *AdminBrokerProcessor) updateBrokerConfig(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)
	logger.Infof("updateBrokerConfig called by %s", remotingUtil.ParseChannelRemoteAddr(ctx))

	content := request.Body
	logger.Infof("BrokerConfig:%s", string(content))
	if content != nil {
		logger.Infof("updateBrokerConfig, new config: %s, client: %s", string(content), ctx.RemoteAddr().String())
		adp.BrokerController.UpdateAllConfig(content)
	} else {
		logger.Error("string2Properties error")
		response.Code = code.SYSTEM_ERROR
		response.Remark = "string2Properties error"
		return response, nil
	}

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// getBrokerConfig 获得Broker配置信息
// Author rongzhihong
// Since 2017/9/19
func (adp *AdminBrokerProcessor) getBrokerConfig(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &header.GetBrokerConfigResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	content := adp.BrokerController.EncodeAllConfig()
	if content != "" && len(content) > 0 {
		response.Body = []byte(content)
	}

	responseHeader.Version = adp.BrokerController.getConfigDataVersion()
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// searchOffsetByTimestamp 根据时间查询偏移量
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) searchOffsetByTimestamp(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &header.SearchOffsetResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	requestHeader := &header.SearchOffsetRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	offset := abp.BrokerController.MessageStore.GetOffsetInQueueByTime(requestHeader.Topic, requestHeader.QueueId, requestHeader.Timestamp)
	responseHeader.Offset = offset
	response.Code = code.SUCCESS
	response.Remark = ""

	return response, nil
}

// getMinOffset 获得最小偏移量
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) getMinOffset(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &header.GetMinOffsetResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	requestHeader := &header.GetMinOffsetRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	offset := abp.BrokerController.MessageStore.GetMinOffsetInQueue(requestHeader.Topic, requestHeader.QueueId)
	responseHeader.Offset = offset
	response.Code = code.SUCCESS
	response.Remark = ""

	return response, nil
}

// getEarliestMsgStoretime 获得最早消息存储时间
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) getEarliestMsgStoretime(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &header.GetEarliestMsgStoretimeResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	requestHeader := &header.GetEarliestMsgStoretimeRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	timestamp := abp.BrokerController.MessageStore.GetEarliestMessageTime(requestHeader.Topic, requestHeader.QueueId)
	responseHeader.Timestamp = timestamp
	response.Code = code.SUCCESS
	response.Remark = ""

	return response, nil
}

// getBrokerRuntimeInfo 获取Broker运行时信息
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) getBrokerRuntimeInfo(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	runtimeInfo := abp.prepareRuntimeInfo()

	kvTable := body.NewKVTable()
	kvTable.Table = runtimeInfo

	content := stgcommon.Encode(kvTable)
	response.Body = content
	response.Code = code.SUCCESS
	response.Remark = ""

	return response, nil
}

// lockBatchMQ 锁队列
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) lockBatchMQ(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	requestBody := body.NewLockBatchRequestBody()
	err := stgcommon.Decode(request.Body, requestBody)
	if err != nil {
		logger.Error(err)
	}

	lockOKMQSet := abp.BrokerController.RebalanceLockManager.TryLockBatch(requestBody.ConsumerGroup,
		requestBody.MqSet, requestBody.ClientId)

	responseBody := body.NewLockBatchResponseBody()
	responseBody.LockOKMQSet = lockOKMQSet

	content := stgcommon.Encode(responseBody)
	response.Body = content
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// lockBatchMQ 解锁队列
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) unlockBatchMQ(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	requestBody := body.NewUnlockBatchRequestBody()
	err := stgcommon.Decode(request.Body, requestBody)
	if err != nil {
		logger.Error(err)
	}

	abp.BrokerController.RebalanceLockManager.UnlockBatch(requestBody.ConsumerGroup, requestBody.MqSet, requestBody.ClientId)

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// prepareRuntimeInfo 组装运行中的Broker的信息
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) prepareRuntimeInfo() map[string]string {
	runtimeInfo := abp.BrokerController.MessageStore.GetRuntimeInfo()
	runtimeInfo["brokerVersionDesc"] = mqversion.GetVersionDesc(mqversion.CurrentVersion)
	runtimeInfo["brokerVersion"] = fmt.Sprintf("%d", mqversion.CurrentVersion)

	runtimeInfo["msgPutTotalYesterdayMorning"] =
		fmt.Sprintf("%d", abp.BrokerController.brokerStats.MsgPutTotalYesterdayMorning)
	runtimeInfo["msgPutTotalTodayMorning"] =
		fmt.Sprintf("%d", abp.BrokerController.brokerStats.MsgPutTotalTodayMorning)
	runtimeInfo["msgPutTotalTodayNow"] =
		fmt.Sprintf("%d", abp.BrokerController.brokerStats.GetMsgPutTotalTodayNow())

	runtimeInfo["msgGetTotalYesterdayMorning"] =
		fmt.Sprintf("%d", abp.BrokerController.brokerStats.MsgGetTotalYesterdayMorning)
	runtimeInfo["msgGetTotalTodayMorning"] =
		fmt.Sprintf("%d", abp.BrokerController.brokerStats.MsgGetTotalTodayMorning)
	runtimeInfo["msgGetTotalTodayNow"] =
		fmt.Sprintf("%d", abp.BrokerController.brokerStats.GetMsgGetTotalTodayNow())

	runtimeInfo["sendThreadPoolQueueCapacity"] =
		fmt.Sprintf("%d", abp.BrokerController.BrokerConfig.SendThreadPoolQueueCapacity)

	return runtimeInfo
}

// updateAndCreateSubscriptionGroup 更新或者创建消费分组
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) updateAndCreateSubscriptionGroup(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	logger.Infof("updateAndCreateSubscriptionGroup called by %s", remotingUtil.ParseChannelRemoteAddr(ctx))

	config := &subscription.SubscriptionGroupConfig{}
	err := stgcommon.Decode(request.Body, config)
	if err != nil {
		logger.Error(err)
	}

	if config != nil {
		abp.BrokerController.SubscriptionGroupManager.UpdateSubscriptionGroupConfig(config)
	}

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// getAllSubscriptionGroup 获得所有消费分组
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) getAllSubscriptionGroup(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)
	content := abp.BrokerController.SubscriptionGroupManager.Encode(false)

	if content != "" && len(content) > 0 {
		response.Body = []byte(content)
	} else {
		logger.Errorf("No subscription group in this broker, client: %s", ctx.RemoteAddr().String())
		response.Code = code.SYSTEM_ERROR
		response.Remark = "No subscription group in this broker"
		return response, nil
	}

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// deleteSubscriptionGroup 删除消费分组
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) deleteSubscriptionGroup(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	requestHeader := &header.DeleteSubscriptionGroupRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	logger.Infof("deleteSubscriptionGroup called by %s", remotingUtil.ParseChannelRemoteAddr(ctx))

	abp.BrokerController.SubscriptionGroupManager.DeleteSubscriptionGroupConfig(requestHeader.GroupName)

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// getTopicStatsInfo 获得Toipc的统计信息
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) getTopicStatsInfo(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)
	requestHeader := &header.GetTopicStatsInfoRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	topic := requestHeader.Topic
	topicConfig := abp.BrokerController.TopicConfigManager.SelectTopicConfig(topic)
	if topicConfig == nil {
		response.Code = code.TOPIC_NOT_EXIST
		response.Remark = fmt.Sprintf("topic[%s] not exist", topic)
		return response, nil
	}

	topicStatsTable := admin.NewTopicStatsTable()

	var writeQueueNums int = int(topicConfig.WriteQueueNums)
	for i := 0; i < writeQueueNums; i++ {
		mq := message.NewMessageQueue()
		mq.Topic = topic
		mq.BrokerName = abp.BrokerController.BrokerConfig.BrokerName
		mq.QueueId = i

		topicOffset := admin.NewTopicOffset()
		min := abp.BrokerController.MessageStore.GetMinOffsetInQueue(topic, int32(i))
		if min < 0 {
			min = 0
		}

		max := abp.BrokerController.MessageStore.GetMaxOffsetInQueue(topic, int32(i))
		if max < 0 {
			max = 0
		}

		timestamp := int64(0)
		if max > 0 {
			// TODO 方法查询有时会卡死 报错:unexpected fault address 0x301012c fatal error: fault
			timestamp = abp.BrokerController.MessageStore.GetMessageStoreTimeStamp(topic, int32(i), (max - 1))
		}

		topicOffset.MinOffset = min
		topicOffset.MaxOffset = max
		topicOffset.LastUpdateTimestamp = timestamp

		topicStatsTable.OffsetTable[mq] = topicOffset
	}
	fmt.Printf("%#v\n", topicStatsTable.OffsetTable)
	content := stgcommon.Encode(&(topicStatsTable.OffsetTable))
	fmt.Println(content)
	response.Code = code.SUCCESS
	response.Body = content
	response.Remark = ""

	return response, nil
}

// getConsumerConnectionList 获得消费者连接信息
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) getConsumerConnectionList(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	requestHeader := &header.GetConsumerConnectionListRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	consumerGroupInfo := abp.BrokerController.ConsumerManager.GetConsumerGroupInfo(requestHeader.ConsumerGroup)
	if consumerGroupInfo != nil {
		bodydata := header.NewConsumerConnection()
		bodydata.ConsumeFromWhere = consumerGroupInfo.ConsumeFromWhere
		bodydata.ConsumeType = consumerGroupInfo.ConsumeType
		bodydata.MessageModel = consumerGroupInfo.MessageModel
		bodydata.SubscriptionTable.PutAll(consumerGroupInfo.SubscriptionTableToMap())

		iterator := consumerGroupInfo.ConnTable.Iterator()
		for iterator.HasNext() {
			_, value, _ := iterator.Next()
			if info, ok := value.(*client.ChannelInfo); ok {
				connection := &header.Connection{}
				connection.ClientId = info.ClientId
				connection.Language = info.LanguageCode
				connection.Version = info.Version
				connection.ClientAddr = remotingUtil.ParseChannelRemoteAddr(info.Context)

				bodydata.ConnectionSet.Add(connection)
			}
		}

		content := stgcommon.Encode(bodydata)
		response.Body = content
		response.Code = code.SUCCESS
		response.Remark = ""
		return response, nil
	}

	response.Code = code.CONSUMER_NOT_ONLINE
	response.Remark = fmt.Sprintf("the consumer group[%s] not online", requestHeader.ConsumerGroup)
	return response, nil
}

// getProducerConnectionList 获得消费者连接信息
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) getProducerConnectionList(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	requestHeader := &header.GetProducerConnectionListRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	channelInfoHashMap := abp.BrokerController.ProducerManager.GroupChannelTable.Get(requestHeader.ProducerGroup)
	if channelInfoHashMap != nil {
		bodydata := body.NewProducerConnection()
		for _, info := range channelInfoHashMap {
			connection := &header.Connection{}
			connection.ClientId = info.ClientId
			connection.Language = info.LanguageCode
			connection.Version = info.Version
			connection.ClientAddr = remotingUtil.ParseChannelRemoteAddr(info.Context)

			bodydata.ConnectionSet.Add(connection)
		}

		content := stgcommon.Encode(bodydata)
		response.Body = content
		response.Code = code.SUCCESS
		response.Remark = ""
		return response, nil
	}

	response.Code = code.CONSUMER_NOT_ONLINE
	response.Remark = fmt.Sprintf("the producer group[%s] not online", requestHeader.ProducerGroup)
	return response, nil
}

// getConsumeStats 获得消费者统计信息
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) getConsumeStats(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	requestHeader := &header.GetConsumeStatsRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	consumeStats := admin.NewConsumeStats()

	topics := set.NewSet()
	if stgcommon.IsBlank(requestHeader.Topic) {
		topics = abp.BrokerController.ConsumerOffsetManager.WhichTopicByConsumer(requestHeader.ConsumerGroup)
	} else {
		topics.Add(requestHeader.Topic)
	}

	for topic := range topics.Iterator().C {

		if topic, ok := topic.(string); ok {

			topicConfig := abp.BrokerController.TopicConfigManager.SelectTopicConfig(topic)
			if nil == topicConfig {
				logger.Warnf("consumeStats, topic config not exist, %s", topic)
				continue
			}

			// Consumer不在线的时候，也允许查询消费进度
			{
				findSubscriptionData := abp.BrokerController.ConsumerManager.FindSubscriptionData(requestHeader.ConsumerGroup, topic)
				// 如果Consumer在线，而且这个topic没有被订阅，那么就跳过
				if nil == findSubscriptionData && abp.BrokerController.ConsumerManager.FindSubscriptionDataCount(
					requestHeader.ConsumerGroup) > 0 {
					logger.Warnf("consumeStats, the consumer group[%s], topic[%s] not exist",
						requestHeader.ConsumerGroup, topic)
					continue
				}
			}

			var writeQueueNums int = int(topicConfig.WriteQueueNums)
			for i := 0; i < writeQueueNums; i++ {
				mq := &message.MessageQueue{}
				mq.Topic = topic
				mq.BrokerName = abp.BrokerController.BrokerConfig.BrokerName
				mq.QueueId = i

				offsetWrapper := &admin.OffsetWrapper{}
				brokerOffset := abp.BrokerController.MessageStore.GetMaxOffsetInQueue(topic, int32(i))
				if brokerOffset < 0 {
					brokerOffset = 0
				}

				consumerOffset := abp.BrokerController.ConsumerOffsetManager.QueryOffset(requestHeader.ConsumerGroup, topic, i)
				if consumerOffset < 0 {
					consumerOffset = 0
				}

				offsetWrapper.BrokerOffset = brokerOffset
				offsetWrapper.ConsumerOffset = consumerOffset

				// 查询消费者最后一条消息对应的时间戳
				timeOffset := consumerOffset - 1
				if timeOffset >= 0 {
					lastTimestamp := abp.BrokerController.MessageStore.GetMessageStoreTimeStamp(topic, int32(i), timeOffset)
					if lastTimestamp > 0 {
						offsetWrapper.LastTimestamp = lastTimestamp
					}
				}

				consumeStats.OffsetTable[mq] = offsetWrapper
			}

			consumeTps := abp.BrokerController.brokerStatsManager.TpsGroupGetNums(requestHeader.ConsumerGroup, topic)
			var consumeTps2 int64 = int64(consumeTps)
			consumeTps2 += consumeStats.ConsumeTps
			consumeStats.ConsumeTps = consumeTps2
		}
	}

	content := stgcommon.Encode(consumeStats)
	response.Body = content
	response.Code = code.SUCCESS
	response.Remark = ""

	return response, nil
}

// getAllConsumerOffset 所有消费者的偏移量
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) getAllConsumerOffset(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	content := abp.BrokerController.ConsumerOffsetManager.Encode(false)
	if content != "" && len(content) > 0 {
		response.Body = []byte(content)
	} else {
		logger.Errorf("No consumer offset in this broker, client: %s", ctx.RemoteAddr().String())
		response.Code = code.SYSTEM_ERROR
		response.Remark = "No consumer offset in this broker"
		return response, nil
	}

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// getAllDelayOffset 所有消费者的定时偏移量
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) getAllDelayOffset(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	content := abp.BrokerController.MessageStore.ScheduleMessageService.Encode()
	if len(content) > 0 {
		response.Body = []byte(content)
	} else {
		logger.Errorf("No delay offset in this broker, client: %s", ctx.RemoteAddr().String())
		response.Code = code.SYSTEM_ERROR
		response.Remark = "No delay offset in this broker"
		return response, nil
	}

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// resetOffset 所有消费者的定时偏移量
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) resetOffset(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	requestHeader := &header.ResetOffsetRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	logger.Infof("[reset-offset] reset offset started by %s. topic=%s, group=%s, timestamp=%s, isForce=%v",
		remotingUtil.ParseChannelRemoteAddr(ctx), requestHeader.Topic, requestHeader.Group, requestHeader.Timestamp, requestHeader.IsForce)

	return abp.BrokerController.Broker2Client.ResetOffset(requestHeader.Topic, requestHeader.Group, requestHeader.Timestamp, requestHeader.IsForce), nil
}

// getConsumerStatus Broker主动获取Consumer端的消息情况
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) getConsumerStatus(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	requestHeader := &header.GetConsumerStatusRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	logger.Infof("[get-consumer-status] get consumer status by %s. topic=%s, group=%s",
		remotingUtil.ParseChannelRemoteAddr(ctx), requestHeader.Topic, requestHeader.Group)

	return abp.BrokerController.Broker2Client.GetConsumeStatus(requestHeader.Topic, requestHeader.Group, requestHeader.ClientAddr), nil
}

// queryTopicConsumeByWho 查询Topic被哪些消费者消费
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) queryTopicConsumeByWho(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	requestHeader := &header.QueryTopicConsumeByWhoRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	// 从订阅关系查询topic被谁消费，只查询在线
	groups := abp.BrokerController.ConsumerManager.QueryTopicConsumeByWho(requestHeader.Topic)

	// 从Offset持久化查询topic被谁消费，离线和在线都会查询
	groupInOffset := abp.BrokerController.ConsumerOffsetManager.WhichGroupByTopic(requestHeader.Topic)
	if groupInOffset != nil {
		groups.Union(groupInOffset)
	}

	groupList := body.NewGroupList()
	groupList.GroupList = groups
	content := stgcommon.Encode(groupList)

	response.Body = content
	response.Code = code.SUCCESS
	response.Remark = ""

	return response, nil
}

// registerFilterServer 注册过滤器
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) registerFilterServer(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &filtersrv.RegisterFilterServerResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	requestHeader := &filtersrv.RegisterFilterServerRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	abp.BrokerController.FilterServerManager.RegisterFilterServer(ctx, requestHeader.FilterServerAddr)

	responseHeader.BrokerId = abp.BrokerController.BrokerConfig.BrokerId
	responseHeader.BrokerName = abp.BrokerController.BrokerConfig.BrokerName

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// queryConsumeTimeSpan 根据 topic 和 group 获取消息的时间跨度
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) queryConsumeTimeSpan(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	requestHeader := &header.QueryConsumeTimeSpanRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	topic := requestHeader.Topic
	topicConfig := abp.BrokerController.TopicConfigManager.SelectTopicConfig(topic)
	if nil == topicConfig {
		response.Code = code.TOPIC_NOT_EXIST
		response.Remark = fmt.Sprintf("topic[%s] not exist", topic)
		return response, nil
	}

	timeSpanSet := set.NewSet()
	var writeQueueNums int = int(topicConfig.WriteQueueNums)
	for i := 0; i < writeQueueNums; i++ {
		timeSpan := &body.QueueTimeSpan{}
		mq := &message.MessageQueue{}
		mq.Topic = topic
		mq.BrokerName = abp.BrokerController.BrokerConfig.BrokerName
		mq.QueueId = i
		timeSpan.MessageQueue = mq

		minTime := abp.BrokerController.MessageStore.GetEarliestMessageTime(topic, int32(i))
		timeSpan.MinTimeStamp = minTime

		max := abp.BrokerController.MessageStore.GetMaxOffsetInQueue(topic, int32(i))
		maxTime := abp.BrokerController.MessageStore.GetMessageStoreTimeStamp(topic, int32(i), (max - 1))

		timeSpan.MaxTimeStamp = maxTime

		var consumeTime int64
		consumerOffset := abp.BrokerController.ConsumerOffsetManager.QueryOffset(requestHeader.Group, topic, i)
		if consumerOffset > 0 {
			consumeTime = abp.BrokerController.MessageStore.GetMessageStoreTimeStamp(topic, int32(i), consumerOffset)
		} else {
			consumeTime = minTime
		}
		timeSpan.ConsumeTimeStamp = consumeTime
		timeSpanSet.Add(timeSpan)
	}

	queryConsumeTimeSpanBody := body.NewQueryConsumeTimeSpanBody()
	queryConsumeTimeSpanBody.ConsumeTimeSpanSet = timeSpanSet
	content := stgcommon.Encode(queryConsumeTimeSpanBody)

	response.Body = content
	response.Code = code.SUCCESS
	response.Remark = ""

	return response, nil
}

// getSystemTopicListFromBroker 从Broker获取系统Topic列表
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) getSystemTopicListFromBroker(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	topics := abp.BrokerController.TopicConfigManager.SystemTopicList

	topicList := body.NewTopicList()
	topicList.TopicList = topics
	content := stgcommon.Encode(topicList)

	response.Body = content
	response.Code = code.SUCCESS
	response.Remark = ""

	return response, nil
}

// cleanExpiredConsumeQueue 删除失效消费队列
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) cleanExpiredConsumeQueue(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	logger.Warn("invoke cleanExpiredConsumeQueue start.")
	abp.BrokerController.MessageStore.CleanExpiredConsumerQueue()
	logger.Warn("invoke cleanExpiredConsumeQueue end.")

	response := protocol.CreateDefaultResponseCommand(nil)
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// getConsumerRunningInfo 调用Consumer，获取Consumer内存数据结构，为监控以及定位问题
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) getConsumerRunningInfo(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	requestHeader := &header.GetConsumerRunningInfoRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}
	return abp.callConsumer(code.GET_CONSUMER_RUNNING_INFO, request, requestHeader.ConsumerGroup, requestHeader.ClientId)
}

// callConsumer call Consumer
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) callConsumer(requestCode int32, request *protocol.RemotingCommand, consumerGroup, clientId string) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	clientChannelInfo := abp.BrokerController.ConsumerManager.FindChannel(consumerGroup, clientId)
	if nil == clientChannelInfo {
		response.Code = code.SYSTEM_ERROR
		response.Remark = fmt.Sprintf("The Consumer <%s> <%s> not online", consumerGroup, clientId)
		return response, nil
	}

	if clientChannelInfo.Version < mqversion.V3_1_8_SNAPSHOT {
		response.Code = code.SYSTEM_ERROR
		response.Remark =
			fmt.Sprintf("The Consumer <%s> Version <%s> too low to finish, please upgrade it to V3_1_8_SNAPSHOT",
				clientId, mqversion.GetVersionDesc(int(clientChannelInfo.Version)))
		return response, nil
	}

	newRequest := protocol.CreateRequestCommand(requestCode, nil)
	newRequest.ExtFields = request.ExtFields
	newRequest.Body = request.Body

	consumerResponse, err := abp.BrokerController.Broker2Client.CallClient(clientChannelInfo.Context, newRequest)
	if err != nil {
		response.Code = code.SYSTEM_ERROR
		response.Remark = fmt.Sprintf("invoke consumer <%s> <%s> Exception: %s", consumerGroup,
			clientId, err.Error())
		return response, nil
	}
	return consumerResponse, nil
}

// queryCorrectionOffset 查找被修正 offset (转发组件）
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) queryCorrectionOffset(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	requestHeader := &header.QueryCorrectionOffsetRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	correctionOffset := abp.BrokerController.ConsumerOffsetManager.QueryMinOffsetInAllGroup(
		requestHeader.Topic, requestHeader.FilterGroups)

	compareOffset := abp.BrokerController.ConsumerOffsetManager.QueryOffsetByGreoupAndTopic(requestHeader.CompareGroup, requestHeader.Topic)

	if compareOffset != nil && len(compareOffset) > 0 {
		for queueId, v := range compareOffset {
			if correctionOffset[queueId] > v {
				correctionOffset[queueId] = stgcommon.MAX_VALUE
			} else {
				correctionOffset[queueId] = v
			}
		}
	}

	correctionBody := body.NewQueryCorrectionOffsetBody()
	correctionBody.CorrectionOffsets = correctionOffset
	content := stgcommon.Encode(correctionBody)

	response.Body = content
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// consumeMessageDirectly consumeMessageDirectly
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) consumeMessageDirectly(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	requestHeader := &header.ConsumeMessageDirectlyResultRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	request.ExtFields["brokerName"] = abp.BrokerController.BrokerConfig.BrokerName
	messageId, err := message.DecodeMessageId(requestHeader.MsgId)
	if err != nil {
		logger.Error(err)
		return nil, nil
	}
	selectMapedBufferResult := abp.BrokerController.MessageStore.SelectOneMessageByOffset(int64(messageId.Offset))
	if nil != selectMapedBufferResult {
		length := selectMapedBufferResult.Size
		readContent := make([]byte, length)
		selectMapedBufferResult.MappedByteBuffer.Read(readContent)
		request.Body = readContent
	}

	return abp.callConsumer(code.CONSUME_MESSAGE_DIRECTLY, request, requestHeader.ConsumerGroup, requestHeader.ClientId)
}

// cloneGroupOffset 克隆cloneGroupOffset
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) cloneGroupOffset(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	requestHeader := &header.CloneGroupOffsetRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	topics := set.NewSet()

	if stgcommon.IsBlank(requestHeader.Topic) {
		topics = abp.BrokerController.ConsumerOffsetManager.WhichTopicByConsumer(requestHeader.SrcGroup)
	} else {
		topics.Add(requestHeader.Topic)
	}

	for item := range topics.Iterator().C {
		if topic, ok := item.(string); ok {
			topicConfig := abp.BrokerController.TopicConfigManager.SelectTopicConfig(topic)
			if nil == topicConfig {
				logger.Warnf("[cloneGroupOffset], topic config not exist, %s", topic)
				continue
			}

			// Consumer不在线的时候，也允许查询消费进度
			if !requestHeader.Offline {
				// 如果Consumer在线，而且这个topic没有被订阅，那么就跳过
				findSubscriptionData := abp.BrokerController.ConsumerManager.FindSubscriptionData(requestHeader.SrcGroup, topic)
				if nil == findSubscriptionData && abp.BrokerController.ConsumerManager.FindSubscriptionDataCount(
					requestHeader.SrcGroup) > 0 {
					logger.Warnf("[cloneGroupOffset], the consumer group[%s], topic[%s] not exist",
						requestHeader.SrcGroup, topic)
					continue
				}
			}

			abp.BrokerController.ConsumerOffsetManager.CloneOffset(requestHeader.SrcGroup, requestHeader.DestGroup, requestHeader.Topic)
		}
	}

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// ViewBrokerStatsData 查看Broker统计信息
// Author rongzhihong
// Since 2017/9/19
func (abp *AdminBrokerProcessor) ViewBrokerStatsData(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)

	requestHeader := &header.ViewBrokerStatsDataRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	statsItem := abp.BrokerController.MessageStore.BrokerStatsManager.GetStatsItem(requestHeader.StatsName, requestHeader.StatsKey)
	if nil == statsItem {
		response.Code = code.SYSTEM_ERROR
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

	content := stgcommon.Encode(brokerStatsData)

	response.Body = content
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

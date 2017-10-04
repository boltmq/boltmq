package test

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgbroker/test/common"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/filtersrv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/subscription"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"strconv"
	"testing"
)

func TestUpdateAndCreateTopic(t *testing.T) {
	var (
		newTopic string = "TopicExample2"
	)

	conntroller := InitBrokerController()
	ctx := common.CreateAdminCtx()

	topicConfigOld := conntroller.TopicConfigManager.TopicConfigSerializeWrapper.TopicConfigTable.Get(newTopic)
	response, err := common.CreateTopic(conntroller, ctx, topicConfigOld, newTopic)
	if err != nil {
		logger.Errorf("common.CreateTopic() err: %s", err.Error())
		t.Fail()
		return
	}

	topicConfigNew := conntroller.TopicConfigManager.TopicConfigSerializeWrapper.TopicConfigTable.Get(newTopic)
	if (topicConfigOld == nil && topicConfigNew != nil) || (*topicConfigOld == *topicConfigNew) {
		logger.Errorf("UpdateAndCreateTopic Failure!")
		t.Fail()
		return
	}

	logger.Infof("old:%s, new:%s", topicConfigOld.ToString(), topicConfigNew.ToString())
	logger.Infof("body:%s, response:%s", string(response.Body), response.ToString())
}

func TestDeleteTopic(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	deleteTopic := "TopicExample"

	topicConfigOld := bc.TopicConfigManager.TopicConfigSerializeWrapper.TopicConfigTable.Get(deleteTopic)
	if topicConfigOld == nil {
		_, err := common.CreateTopic(bc, ctx, topicConfigOld, deleteTopic)
		if err != nil {
			t.Fail()
			t.Error(err)
		}
	}
	topicConfigOld = bc.TopicConfigManager.TopicConfigSerializeWrapper.TopicConfigTable.Get(deleteTopic)
	if topicConfigOld == nil {
		t.Fail()
		t.Error("创建Topic失败")
	}

	requestHeader := &header.DeleteTopicRequestHeader{}
	requestHeader.Topic = deleteTopic

	response, err := common.ProcessRequest(bc, ctx, code.DELETE_TOPIC_IN_BROKER, requestHeader)
	if err != nil {
		t.Error(err)
	}

	topicConfigNew := bc.TopicConfigManager.TopicConfigSerializeWrapper.TopicConfigTable.Get(deleteTopic)
	if topicConfigNew != nil {
		t.Fail()
		t.Error("删除失败")
	}

	t.Logf("old:%v, new:%v", topicConfigOld, topicConfigNew)
	t.Logf(" body:%s, response:%#v", response.Body, response)
}

func TestGetAllTopicConfig(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	response, err := common.ProcessRequest(bc, ctx, code.GET_ALL_TOPIC_CONFIG, nil)
	if err != nil {
		t.Error(err)
	}

	topicConfig := body.NewTopicConfigSerializeWrapper()
	stgcommon.Decode(response.Body, topicConfig)
	t.Logf("topicConfig:%#v version:%#v\n", topicConfig.TopicConfigTable, topicConfig.DataVersion)
}

func TestUpdateBrokerConfig(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	brokerName := bc.BrokerConfig.BrokerName
	accessMessageInMemoryMaxRatio := bc.MessageStoreConfig.AccessMessageInMemoryMaxRatio

	configContent := bc.EncodeAllConfig()

	allConfig := stgbroker.NewBrokerAllConfig()
	stgcommon.Decode([]byte(configContent), allConfig)

	allConfig.BrokerConfig.BrokerName = "UpdateBrokerName"
	allConfig.MessageStoreConfig.AccessMessageInMemoryMaxRatio = 50

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	request := protocol.CreateRequestCommand(code.UPDATE_BROKER_CONFIG, nil)
	request.Body = stgcommon.Encode(allConfig)

	_, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Error(err)
	}

	t.Logf(" brokerName old:%s, new:%s; accessMessageInMemoryMaxRatio old:%d, new:%d", brokerName, bc.BrokerConfig.BrokerName, accessMessageInMemoryMaxRatio, bc.MessageStoreConfig.AccessMessageInMemoryMaxRatio)

	if bc.MessageStoreConfig.AccessMessageInMemoryMaxRatio == accessMessageInMemoryMaxRatio {
		t.Fail()
		t.Errorf("更新MessageStoreConfig.AccessMessageInMemoryMaxRatio失败, old: %d, new:%d", accessMessageInMemoryMaxRatio, bc.MessageStoreConfig.AccessMessageInMemoryMaxRatio)
		return
	}

	if bc.BrokerConfig.BrokerName == brokerName { // 40 == 50
		t.Fail()
		t.Errorf("更新BrokerConfig.BrokerName失败, old: %d, new:%d", brokerName, bc.BrokerConfig.BrokerName)
	}
}

func TestGetBrokerConfig(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	request := protocol.CreateRequestCommand(code.GET_BROKER_CONFIG, nil)

	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil || response.Body == nil {
		t.Fail()
		t.Error(err)
	}

	t.Logf("Version:%d, Config:%s", response.Version, string(response.Body))
}

func TestSearchOffsetByTimestamp(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &header.SearchOffsetRequestHeader{}
	requestHeader.Topic = "TestTopic"
	requestHeader.QueueId = 0
	requestHeader.Timestamp = 1506682913490

	response, err := common.ProcessRequest(bc, ctx, code.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader)

	if err != nil {
		t.Fail()
		t.Error(err)
	}
	// TODO:查询报错
	t.Logf("ExtFields:%v", response.ExtFields)

}

func TestGetMaxOffset(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := header.NewGetMaxOffsetRequestHeader()
	requestHeader.QueueId = 0
	requestHeader.Topic = "TestTopic"

	request := protocol.CreateRequestCommand(code.GET_MAX_OFFSET, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)

	response, err := adminProcessor.ProcessRequest(ctx, request)
	response.EncodeHeader()
	t.Logf("Offset:%v", response.CustomHeader)

	maxOffset := response.ExtFields["Offset"]

	if err != nil || maxOffset == "" {
		t.Fail()
		t.Errorf("maxOffset为0或者 error:%v", err)
		return
	}
	offset, err := strconv.Atoi(maxOffset)
	if err != nil || offset == 0 {
		t.Fail()
		t.Error("如存在存储文件，则最大偏移量不应该为0")
	}

}

func TestGetMinOffset(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &header.GetMinOffsetRequestHeader{}
	requestHeader.QueueId = 0
	requestHeader.Topic = "TestTopic"

	request := protocol.CreateRequestCommand(code.GET_MIN_OFFSET, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)

	response, err := adminProcessor.ProcessRequest(ctx, request)
	response.EncodeHeader()
	minOffset := response.ExtFields["Offset"]

	t.Logf("Offset:%v", response.CustomHeader)
	if err != nil || minOffset == "" {
		t.Fail()
		t.Errorf("minOffset不为0或者 error:%v", err)
		return
	}
	offset, err := strconv.Atoi(minOffset)
	if err != nil || offset != 0 {
		t.Fail()
		t.Error("最小偏移量应该为0")
	}

}

func TestGetEarliestMsgStoretime(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &header.GetEarliestMsgStoretimeRequestHeader{}
	requestHeader.QueueId = 0
	requestHeader.Topic = "TestTopic"

	request := protocol.CreateRequestCommand(code.GET_EARLIEST_MSG_STORETIME, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)

	response, err := adminProcessor.ProcessRequest(ctx, request)
	response.EncodeHeader()
	t.Logf("CustomHeader:%v, response:%v", response.CustomHeader, response)

	timestamp := response.ExtFields["Timestamp"]
	if err != nil || timestamp == "" {
		t.Fail()
		t.Errorf("查询时间异常,%v", err)
	}

	// TODO 当前MessageStore.GetEarliestMessageTime查询到的时间都为-1
	mills, err := strconv.Atoi(timestamp)
	if err != nil || mills == -1 { // 查不到则mills值为-1
		t.Fail()
		t.Error("最小偏移量应该为0")
	}
}

func TestGetBrokerRuntimeInfo(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	request := protocol.CreateRequestCommand(code.GET_BROKER_RUNTIME_INFO, nil)

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Fail()
		t.Error(err)
		return
	}

	if response.Body == nil || len(response.Body) <= 0 {
		t.Fail()
		t.Error("Broker运行信息查询错误")
	}
	t.Logf("Body:%v", string(response.Body))
}

func TestUpdateAndCreateSubscriptionGroup(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()
	newSubscriptionGroup := "groupNameTest"

	subscriptionGroupConfigOld := bc.SubscriptionGroupManager.SubscriptionGroupTable.Get(newSubscriptionGroup)

	request := protocol.CreateRequestCommand(code.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, nil)
	config := &subscription.SubscriptionGroupConfig{GroupName: newSubscriptionGroup, ConsumeEnable: true,
		ConsumeFromMinEnable: true, ConsumeBroadcastEnable: true, RetryQueueNums: 1, RetryMaxTimes: 15,
		BrokerId: 0, WhichBrokerWhenConsumeSlowly: 0}

	if subscriptionGroupConfigOld != nil {
		config.ConsumeEnable = !subscriptionGroupConfigOld.ConsumeEnable
	}

	request.Body = stgcommon.Encode(config)

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Fail()
		t.Error(err)
		return
	}

	subscriptionGroupConfigNew := bc.SubscriptionGroupManager.SubscriptionGroupTable.Get(newSubscriptionGroup)
	if subscriptionGroupConfigNew == nil || (subscriptionGroupConfigOld != nil && subscriptionGroupConfigNew != nil &&
		*subscriptionGroupConfigOld == *subscriptionGroupConfigNew) {
		t.Fail()
		t.Error("创建或更新订阅关系失败.")
	}
	t.Logf("response:%v", response)
}

func TestGetAllSubscriptionGroup(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	request := protocol.CreateRequestCommand(code.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, nil)

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Fail()
		t.Error(err)
		return
	}

	if response.Body == nil || len(response.Body) <= 0 {
		t.Fail()
		t.Error("获取所有订阅信息错误")
	}
	t.Logf("response Body:%s", string(response.Body))
}

func TestDeleteSubscriptionGroup(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	deleteSubscriptionGroup := "deleteGroupNameTest"

	subscriptionGroupConfigOld := bc.SubscriptionGroupManager.SubscriptionGroupTable.Get(deleteSubscriptionGroup)
	if subscriptionGroupConfigOld == nil {
		request := protocol.CreateRequestCommand(code.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, nil)
		config := &subscription.SubscriptionGroupConfig{GroupName: deleteSubscriptionGroup, ConsumeEnable: true,
			ConsumeFromMinEnable: true, ConsumeBroadcastEnable: true, RetryQueueNums: 1, RetryMaxTimes: 15,
			BrokerId: 0, WhichBrokerWhenConsumeSlowly: 0}

		request.Body = stgcommon.Encode(config)

		adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
		adminProcessor.ProcessRequest(ctx, request)

		subscriptionGroupConfigOld = bc.SubscriptionGroupManager.SubscriptionGroupTable.Get(deleteSubscriptionGroup)
		if subscriptionGroupConfigOld == nil {
			t.Fail()
			t.Error("创建订阅关系失败")
			return
		}
	}

	requestHeader := &header.DeleteSubscriptionGroupRequestHeader{}
	requestHeader.GroupName = deleteSubscriptionGroup

	delRequest := protocol.CreateRequestCommand(code.DELETE_SUBSCRIPTIONGROUP, requestHeader)
	delRequest.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, delRequest)
	if err != nil {
		t.Fail()
		t.Error(err)
		return
	}

	subscriptionGroupConfigNew := bc.SubscriptionGroupManager.SubscriptionGroupTable.Get(deleteSubscriptionGroup)
	if subscriptionGroupConfigOld != nil && subscriptionGroupConfigNew == nil {
		// to do nothing
	} else {
		t.Fail()
		t.Error("订阅关系删除失败")
	}
	t.Logf("response:%v", response)
}

func TestGetTopicStatsInfo(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &header.GetTopicStatsInfoRequestHeader{}
	requestHeader.Topic = "TestTopic"

	request := protocol.CreateRequestCommand(code.GET_TOPIC_STATS_INFO, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Error(err)
	}
	// TODO MessageStore.GetMessageStoreTimeStamp报错
	t.Logf("response.Body:%s", string(response.Body))
}

func TestGetConsumerConnectionList(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &header.GetConsumerConnectionListRequestHeader{}
	requestHeader.ConsumerGroup = "myConsumerGroup"

	request := protocol.CreateRequestCommand(code.GET_CONSUMER_CONNECTION_LIST, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Fail()
		t.Error(err)
		return
	}

	if response.Body == nil || len(string(response.Body)) <= 0 {
		t.Fail()
		t.Error("获得消费者连接列表为空")
	}
	t.Logf("Body:%s, response:%#v", response.Body, response)
}

func TestGetProducerConnectionList(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &header.GetProducerConnectionListRequestHeader{}
	requestHeader.ProducerGroup = "producer"

	request := protocol.CreateRequestCommand(code.GET_PRODUCER_CONNECTION_LIST, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Fail()
		t.Error(err)
		return
	}

	if response.Body == nil || len(string(response.Body)) <= 0 {
		t.Fail()
		t.Error("获得生产者连接列表为空")
	}
	t.Logf("Body:%s, response:%#v", response.Body, response)
}

func TestGetConsumeStats(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &header.GetConsumeStatsRequestHeader{}
	requestHeader.ConsumerGroup = "myConsumerGroup"
	requestHeader.Topic = "TestTopic"

	request := protocol.CreateRequestCommand(code.GET_CONSUME_STATS, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Fail()
		t.Error(err)
		return
	}
	// TODO json.Encode error:json: unsupported type: map[*message.MessageQueue]*admin.OffsetWrapper
	if response.Body == nil || len(string(response.Body)) <= 0 {
		t.Fail()
		t.Error("获得消费者的统计信息失败")
	}

	t.Logf("Body:%s, response:%v", string(response.Body), response)
}

func TestGetAllConsumerOffset(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	request := protocol.CreateRequestCommand(code.GET_ALL_CONSUMER_OFFSET, nil)

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Fail()
		t.Error(err)
		return
	}

	if response.Body == nil || len(string(response.Body)) <= 0 {
		t.Fail()
		t.Error("获得的所有消费者消费信息为空")
	}
	t.Logf("Body:%s, response:%v", string(response.Body), response)
}

func TestGetAllDelayOffset(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	request := protocol.CreateRequestCommand(code.GET_ALL_DELAY_OFFSET, nil)

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Fail()
		t.Error(err)
		return
	}
	// TODO MessageStore.ScheduleMessageService.Encode()报错

	if response.Body == nil || len(string(response.Body)) <= 0 {
		t.Fail()
		t.Error("获得的所有消费者消费信息为空")
	}
	t.Logf("Body:%s, response:%v", string(response.Body), response)
}

func TestResetOffset(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()
	requestHeader := &header.ResetOffsetRequestHeader{}
	requestHeader.Topic = "TestTopic"
	requestHeader.Group = "myConsumerGroup"
	requestHeader.Timestamp = timeutil.CurrentTimeMillis()
	requestHeader.IsForce = false

	request := protocol.CreateRequestCommand(code.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil || response.Code != 0 {
		t.Fail()
		t.Errorf("error:%v, remarks:%s", err, response.Remark)
		return
	}

	if response.Body == nil || len(string(response.Body)) <= 0 {
		t.Fail()
		t.Error("获得的客户端的offset重置信息为空")
	}

	t.Logf("Body:%s, response:%v", string(response.Body), response)
}

func TestGetConsumerStatus(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &header.GetConsumerStatusRequestHeader{}
	requestHeader.Topic = "TestTopic"
	requestHeader.Group = "myConsumerGroup"
	requestHeader.ClientAddr = "127.0.0.1:56501"

	request := protocol.CreateRequestCommand(code.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil || response.Code != 0 {
		t.Fail()
		t.Errorf("error:%v, remarks:%s", err, response.Remark)
		return
	}

	if response.Body == nil || len(string(response.Body)) <= 0 {
		t.Fail()
		t.Error("获取Consumer端的消息情况为空")
	}

	t.Logf("Body:%s, response:%v", string(response.Body), response)
}

func TestQueryTopicConsumeByWho(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &header.QueryTopicConsumeByWhoRequestHeader{}
	requestHeader.Topic = "TestTopic"

	request := protocol.CreateRequestCommand(code.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil || response.Code != 0 {
		t.Fail()
		t.Errorf("error:%v, remarks:%s", err, response.Remark)
		return
	}

	if response.Body == nil || len(string(response.Body)) <= 0 {
		t.Fail()
		t.Error("查询Topic被哪些消费者消费的列表为空")
	}

	t.Logf("Body:%s, response:%v", string(response.Body), response)
}

func TestRegisterFilterServer(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &filtersrv.RegisterFilterServerRequestHeader{}
	requestHeader.FilterServerAddr = "127.0.0.1"

	request := protocol.CreateRequestCommand(code.REGISTER_FILTER_SERVER, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil || response.Code != 0 {
		t.Fail()
		t.Errorf("error:%v, remarks:%s", err, response.Remark)
		return
	}

	t.Logf("Body:%s, response:%v", string(response.Body), response)
}

func TestQueryConsumeTimeSpan(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &header.QueryConsumeTimeSpanRequestHeader{}
	requestHeader.Topic = "TestTopic"
	requestHeader.Group = "myConsumerGroup"

	request := protocol.CreateRequestCommand(code.QUERY_CONSUME_TIME_SPAN, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil || response.Code != 0 {
		t.Fail()
		t.Errorf("error:%v, remarks:%s", err, response.Remark)
		return
	}

	// TODO MessageStore.GetMessageStoreTimeStamp报错
	t.Logf("Body:%s, response:%v", string(response.Body), response)
}

func TestGetSystemTopicListFromBroker(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	request := protocol.CreateRequestCommand(code.GET_SYSTEM_TOPIC_LIST_FROM_BROKER, nil)

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil || response.Code != 0 {
		t.Fail()
		t.Errorf("error:%v, remarks:%s", err, response.Remark)
		return
	}

	if response.Body == nil || len(string(response.Body)) <= 0 {
		t.Fail()
		t.Error("从Broker获取系统Topic列表为空")
	}
	t.Logf("Body:%s, response:%v", string(response.Body), response)
}

func TestCleanExpiredConsumeQueue(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	request := protocol.CreateRequestCommand(code.CLEAN_EXPIRED_CONSUMEQUEUE, nil)

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil || response.Code != 0 {
		t.Fail()
		t.Errorf("error:%v, remarks:%s", err, response.Remark)
		return
	}

	t.Logf("Body:%s, response:%v", string(response.Body), response)
}

func TestGetConsumerRunningInfo(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &header.GetConsumerRunningInfoRequestHeader{}
	requestHeader.ClientId = "0"
	requestHeader.ConsumerGroup = "myConsumerGroup"
	requestHeader.JstackEnable = false

	request := protocol.CreateRequestCommand(code.GET_CONSUMER_RUNNING_INFO, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil || response.Code != 0 {
		t.Fail()
		t.Errorf("error:%v, remarks:%s", err, response.Remark)
		return
	}

	t.Logf("Body:%s, response:%v", string(response.Body), response)
}

func TestQueryCorrectionOffset(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &header.QueryCorrectionOffsetRequestHeader{}
	requestHeader.FilterGroups = "myFilterGroup"
	requestHeader.CompareGroup = "myConsumerGroup"
	requestHeader.Topic = "TestTopic"

	request := protocol.CreateRequestCommand(code.QUERY_CORRECTION_OFFSET, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil || response.Code != 0 {
		t.Fail()
		t.Errorf("error:%v, remarks:%s", err, response.Remark)
		return
	}

	if response.Body == nil || len(string(response.Body)) <= 0 {
		t.Fail()
		t.Error("查找被修正的offset列表为空")
	}

	t.Logf("Body:%s, response:%v", string(response.Body), response)
}

func TestConsumeMessageDirectly(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &header.ConsumeMessageDirectlyResultRequestHeader{}
	requestHeader.ConsumerGroup = "myConsumerGroup"
	requestHeader.BrokerName = "broker-master2"
	requestHeader.ClientId = "0"
	requestHeader.MsgId = "0A7A011100002A9F000000000000007A"

	request := protocol.CreateRequestCommand(code.CONSUME_MESSAGE_DIRECTLY, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil || response.Code != 0 {
		t.Fail()
		t.Errorf("error:%v, remarks:%s", err, response.Remark)
		return
	}

	t.Logf("Body:%s, response:%v", string(response.Body), response)
}

func TestCloneGroupOffset(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &header.CloneGroupOffsetRequestHeader{}
	requestHeader.Topic = "TestTopic"
	requestHeader.Offline = false
	requestHeader.SrcGroup = "myConsumerGroup"
	requestHeader.DestGroup = "otherConsumerGroup"

	request := protocol.CreateRequestCommand(code.CLONE_GROUP_OFFSET, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil || response.Code != 0 {
		t.Fail()
		t.Errorf("error:%v, remarks:%s", err, response.Remark)
		return
	}

	t.Logf("Body:%s, response:%v", string(response.Body), response)
}

func TestViewBrokerStatsData(t *testing.T) {
	bc := InitBrokerController()
	ctx := common.CreateAdminCtx()

	requestHeader := &header.ViewBrokerStatsDataRequestHeader{}
	requestHeader.StatsKey = "TestTopic"
	requestHeader.StatsName = "TOPIC_PUT_NUMS"

	request := protocol.CreateRequestCommand(code.VIEW_BROKER_STATS_DATA, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil || response.Code != 0 {
		t.Fail()
		t.Errorf("error:%v, remarks:%s", err, response.Remark)
		return
	}

	if response.Body == nil || len(string(response.Body)) <= 0 {
		t.Fail()
		t.Error("查找到的Broker统计信息为空")
	}

	t.Logf("Body:%s, response:%v", string(response.Body), response)
}

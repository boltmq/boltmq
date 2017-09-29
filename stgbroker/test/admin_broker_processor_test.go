package test

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	protocol2 "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/subscription"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"testing"
)

func TestUpdateAndCreateTopic(t *testing.T) {
	bc := InitBrokerController()
	ctx := createAdminCtx()

	requestHeader := &header.CreateTopicRequestHeader{}
	requestHeader.Topic = "TopicExample"
	requestHeader.DefaultTopic = "defaultBroker"
	requestHeader.ReadQueueNums = 12
	requestHeader.WriteQueueNums = 12
	requestHeader.Perm = 7
	requestHeader.TopicFilterType = stgcommon.MULTI_TAG
	requestHeader.TopicSysFlag = 4
	requestHeader.Order = false

	request := protocol.CreateRequestCommand(protocol2.UPDATE_AND_CREATE_TOPIC, requestHeader)
	// 调用EncodeHeader方法中的makeCustomHeaderToNet方法:将requestHeader的值写入到ExtFields中
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Error(err)
	}
	t.Log(response)
}

func TestDeleteTopic(t *testing.T) {
	bc := InitBrokerController()
	ctx := createAdminCtx()

	requestHeader := &header.DeleteTopicRequestHeader{}
	requestHeader.Topic = "TopicExample"

	request := protocol.CreateRequestCommand(protocol2.DELETE_TOPIC_IN_BROKER, requestHeader)
	// 调用EncodeHeader方法中的makeCustomHeaderToNet方法:将requestHeader的值写入到ExtFields中
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Error(err)
	}
	t.Log(response)
}

func TestGetAllTopicConfig(t *testing.T) {
	bc := InitBrokerController()
	ctx := createAdminCtx()

	request := protocol.CreateRequestCommand(protocol2.GET_ALL_TOPIC_CONFIG, nil)

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Error(err)
	}
	topicConfig := body.NewTopicConfigSerializeWrapper()
	stgcommon.Decode(response.Body, topicConfig)
	fmt.Printf("topicConfig:%#v version:%#v\n", topicConfig.TopicConfigTable, topicConfig.DataVersion)
	t.Log(response)
}

func TestUpdateBrokerConfig(t *testing.T) {
	bc := InitBrokerController()
	ctx := createAdminCtx()

	brokerName := bc.BrokerConfig.BrokerName
	accessMessageInMemoryMaxRatio := bc.MessageStoreConfig.AccessMessageInMemoryMaxRatio

	configContent := bc.EncodeAllConfig()

	allConfig := stgbroker.NewBrokerAllConfig()
	stgcommon.Decode([]byte(configContent), allConfig)

	allConfig.BrokerConfig.BrokerName = "UpdateBrokerName"
	allConfig.MessageStoreConfig.AccessMessageInMemoryMaxRatio = 50

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	request := protocol.CreateRequestCommand(protocol2.UPDATE_BROKER_CONFIG, nil)
	request.Body = stgcommon.Encode(allConfig)

	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Error(err)
	}
	t.Log(response)

	if bc.MessageStoreConfig.AccessMessageInMemoryMaxRatio == accessMessageInMemoryMaxRatio {
		t.Errorf("更新MessageStoreConfig.AccessMessageInMemoryMaxRatio失败, old: %d, new:%d",
			accessMessageInMemoryMaxRatio, bc.MessageStoreConfig.AccessMessageInMemoryMaxRatio)
		return
	}

	if bc.BrokerConfig.BrokerName == brokerName {
		t.Errorf("更新BrokerConfig.BrokerName失败, old: %d, new:%d",
			brokerName, bc.BrokerConfig.BrokerName)
		return
	}
	t.Log("更新成功！")
}

func TestGetBrokerConfig(t *testing.T) {
	bc := InitBrokerController()
	ctx := createAdminCtx()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	request := protocol.CreateRequestCommand(protocol2.GET_BROKER_CONFIG, nil)

	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Error(err)
	}
	t.Logf("Version:%d, Config:%s", response.Version, string(response.Body))

}

func TestSearchOffsetByTimestamp(t *testing.T) {
	bc := InitBrokerController()
	ctx := createAdminCtx()

	requestHeader := &header.SearchOffsetRequestHeader{}
	requestHeader.Topic = "TestTopic"
	requestHeader.QueueId = 7
	requestHeader.Timestamp = timeutil.CurrentTimeMillis()

	request := protocol.CreateRequestCommand(protocol2.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	// TODO proccessor中Timestamp时间解析错误
	response, err := adminProcessor.ProcessRequest(ctx, request)

	if err != nil {
		t.Error(err)
	}
	t.Logf("ExtFields:%v", response.ExtFields)

}

func TestGetMaxOffset(t *testing.T) {
	bc := InitBrokerController()
	ctx := createAdminCtx()

	requestHeader := header.NewGetMaxOffsetRequestHeader()
	requestHeader.QueueId = 0
	requestHeader.Topic = "TestTopic"

	request := protocol.CreateRequestCommand(protocol2.GET_MAX_OFFSET, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)

	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Error(err)
	}
	t.Logf("ExtFields:%v", response.ExtFields)
	// TODO no pass

}

func TestGetMinOffset(t *testing.T) {
	bc := InitBrokerController()
	ctx := createAdminCtx()

	requestHeader := &header.GetMinOffsetRequestHeader{}
	requestHeader.QueueId = 0
	requestHeader.Topic = "TestTopic"

	request := protocol.CreateRequestCommand(protocol2.GET_MIN_OFFSET, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)

	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Error(err)
	}
	t.Logf("ExtFields:%v", response.ExtFields)
	// TODO no pass
}

func TestGetEarliestMsgStoretime(t *testing.T) {
	bc := InitBrokerController()
	ctx := createAdminCtx()

	requestHeader := &header.GetEarliestMsgStoretimeRequestHeader{}
	requestHeader.QueueId = 0
	requestHeader.Topic = "TestTopic"

	request := protocol.CreateRequestCommand(protocol2.GET_EARLIEST_MSG_STORETIME, requestHeader)
	request.EncodeHeader()

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)

	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Error(err)
	}
	t.Logf("ExtFields:%v", response)
	// TODO no pass
}

func TestGetBrokerRuntimeInfo(t *testing.T) {
	bc := InitBrokerController()
	ctx := createAdminCtx()

	request := protocol.CreateRequestCommand(protocol2.GET_BROKER_RUNTIME_INFO, nil)

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Error(err)
	}
	t.Logf("Body:%v", string(response.Body))
}

func TestUpdateAndCreateSubscriptionGroup(t *testing.T) {
	bc := InitBrokerController()
	ctx := createAdminCtx()

	request := protocol.CreateRequestCommand(protocol2.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, nil)
	config := &subscription.SubscriptionGroupConfig{GroupName: "groupNameTest", ConsumeEnable: true,
		ConsumeFromMinEnable: true, ConsumeBroadcastEnable: true, RetryQueueNums: 1, RetryMaxTimes: 16,
		BrokerId: 0, WhichBrokerWhenConsumeSlowly: 0}

	request.Body = stgcommon.Encode(config)

	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Error(err)
	}
	t.Logf("response:%v", response)
}

func createAdminCtx() netm.Context {
	var remoteContext netm.Context

	bootstrap := netm.NewBootstrap()
	go bootstrap.Bind("127.0.0.1", 18002).
		RegisterHandler(func(buffer []byte, ctx netm.Context) {
			remoteContext = ctx
		}).Sync()

	clientBootstrap := netm.NewBootstrap()
	err := clientBootstrap.Connect("127.0.0.1", 18002)
	if err != nil {
		logger.Error(err)
	}
	return clientBootstrap.Contexts()[0]
}

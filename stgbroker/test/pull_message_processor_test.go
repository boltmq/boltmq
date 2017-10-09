package test

import (
	"errors"
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
	"testing"
)

// TestPullRequest 测试拉取消息
// Author rongzhihong
// Since 2017/9/25
func TestPullRequest(t *testing.T) {
	bc := InitBrokerController()
	bc.MessageStore.Start()

	ctx := createCtx()
	request := createRequest()

	response, err := bc.PullMessageProcessor.ProcessRequest(ctx, request)
	if err != nil {
		t.Error(err)
	}
	if response != nil {
		t.Log(response)
	}
}

func createRequest() *protocol.RemotingCommand {
	requestHeader := &header.PullMessageRequestHeader{}
	requestHeader.ConsumerGroup = "myConsumerGroup"
	requestHeader.Topic = "TestTopic"
	requestHeader.QueueId = 0
	requestHeader.QueueOffset = 0
	requestHeader.MaxMsgNums = 32
	requestHeader.SysFlag = 4
	requestHeader.CommitOffset = 0
	requestHeader.SuspendTimeoutMillis = 2000
	requestHeader.Subscription = "tagA"
	requestHeader.SubVersion = 0

	request := protocol.CreateRequestCommand(1, requestHeader)
	request.Code = 11
	request.Language = "GOLANG"
	request.Version = 79
	request.Opaque = 6
	request.Flag = 0
	request.Remark = ""

	request.ExtFields = map[string]string{
		"MaxMsgNums":           "32",
		"CommitOffset":         "0",
		"Subscription":         "tagA",
		"Topic":                "TestTopic",
		"QueueId":              "3",
		"SuspendTimeoutMillis": "2000",
		"SubVersion":           "0",
		"ConsumerGroup":        "myConsumerGroup",
		"SysFlag":              "4",
		"QueueOffset":          "0",
	}

	return request
}

func InitBrokerController() *stgbroker.BrokerController {
	// 初始化brokerConfig
	brokerConfig := stgcommon.NewBrokerConfig("BrokerName", "BrokerClusterName")

	// 初始化messageStoreConfig
	messageStoreConfig := stgstorelog.NewMessageStoreConfig()

	// 创建BrokerController结构体
	remotingClient := remoting.NewDefalutRemotingClient()
	controller := stgbroker.NewBrokerController(brokerConfig, messageStoreConfig, remotingClient)

	// 初始化controller
	initResult := controller.Initialize()
	if !initResult {
		controller.Shutdown()
		panic(errors.New("init fail"))
	}

	return controller
}

func createCtx() netm.Context {
	var remoteContext netm.Context

	bootstrap := netm.NewBootstrap()
	go bootstrap.Bind("127.0.0.1", 18001).
		RegisterHandler(func(buffer []byte, ctx netm.Context) {
			remoteContext = ctx
		}).Sync()

	clientBootstrap := netm.NewBootstrap()
	err := clientBootstrap.Connect("127.0.0.1", 18001)
	if err != nil {
		logger.Error(err)
	}
	return clientBootstrap.Contexts()[0]
}

package stgbroker

import (
	"errors"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker/mqtrace"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
	"testing"
	commonprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
)

// TestPutMessage 测试存消息
// Author rongzhihong
// Since 2017/9/25
func TestPutMessage(t *testing.T) {
	bc := InitBrokerController()
	bc.MessageStore.Start()

	_, requestHeader := CreateSendMessageRequest()
	msgInner := new(stgstorelog.MessageExtBrokerInner)
	msgInner.Topic = requestHeader.Topic
	msgInner.Body = []byte("store message")
	msgInner.Flag = requestHeader.Flag
	message.SetPropertiesMap(&msgInner.Message, message.String2messageProperties(requestHeader.Properties))
	msgInner.PropertiesString = requestHeader.Properties
	msgInner.TagsCode = stgstorelog.TagsString2tagsCode(nil, msgInner.GetTags())
	msgInner.QueueId = 0
	msgInner.SysFlag = 8
	msgInner.BornTimestamp = requestHeader.BornTimestamp
	msgInner.BornHost = "127.0.0.1"
	msgInner.StoreHost = "127.0.0.1"

	putMessageResult := bc.MessageStore.PutMessage(msgInner)
	if putMessageResult == nil || putMessageResult.PutMessageStatus != 0 {
		t.Error("消息存入失败！")
		return
	}

	if putMessageResult != nil && putMessageResult.PutMessageStatus == 0 {
		logger.Infof("消息存入成功！ putMessageResult:%#v", putMessageResult)
	}

}

// TestPutMessage 测试SendMessage方法
// Author rongzhihong
// Since 2017/9/25
func TestSendMessage(t *testing.T) {
	brokerController := InitBrokerController()
	brokerController.MessageStore.Start()

	sendMessage := NewSendMessageProcessor(brokerController)

	ctx := CreateCtx()
	request, requestHeader := CreateSendMessageRequest()
	mqTraceContext := CreateMqtraceContext()

	respone := sendMessage.sendMessage(ctx, request, mqTraceContext, requestHeader)
	fmt.Println(respone)
}

// TestConsumerSendMsgBack 测试consumerSendMsgBack方法
// Author rongzhihong
// Since 2017/9/25
func TestConsumerSendMsgBack(t *testing.T) {
	brokerController := InitBrokerController()
	brokerController.MessageStore.Start()

	sendMessage := NewSendMessageProcessor(brokerController)

	ctx := CreateCtx()

	request := CreateConsumerSendMsgBackRequest()

	respone := sendMessage.consumerSendMsgBack(ctx, request)
	fmt.Println(respone)
}

func InitBrokerController() *BrokerController {
	// 初始化brokerConfig
	brokerConfig := stgcommon.NewBrokerConfig()

	brokerConfig.BrokerName = "BrokerName"
	brokerConfig.BrokerClusterName = "BrokerClusterName"

	// 初始化brokerConfig
	messageStoreConfig := stgstorelog.NewMessageStoreConfig()

	controller := NewBrokerController(*brokerConfig, messageStoreConfig)

	// 初始化controller
	initResult := controller.Initialize()
	if !initResult {
		controller.Shutdown()
		panic(errors.New("init fail"))
	}

	return controller
}

func CreateCtx() netm.Context {
	var remoteContext netm.Context

	bootstrap := netm.NewBootstrap()
	go bootstrap.Bind("127.0.0.1", 18000).
		RegisterHandler(func(buffer []byte, ctx netm.Context) {
		remoteContext = ctx
	}).Sync()

	clientBootstrap := netm.NewBootstrap()
	err := clientBootstrap.Connect("127.0.0.1", 18000)
	if err != nil {
		logger.Error(err)
	}
	return clientBootstrap.Contexts()[0]
}

func CreateSendMessageRequest() (*protocol.RemotingCommand, *header.SendMessageRequestHeader) {
	var requestHeader = &header.SendMessageRequestHeader{ProducerGroup: "producer", Topic: "TestTopic",
		DefaultTopic: "", DefaultTopicQueueNums: 4, QueueId: 0, SysFlag: 0, BornTimestamp: 1506422161000, Flag: 0,
		Properties: "TAGS:tagA", ReconsumeTimes: 0, UnitMode: false}

	var request = protocol.CreateRequestCommand(310, requestHeader)
	request.Language = "GOLANG"
	request.Version = 79
	request.Opaque = 8
	request.Flag = 0
	request.Remark = ""

	extFields := map[string]string{"A": "producer", "B": "TestTopic", "C": "MY_DEFAULT_TOPIC",
		"D": "4", "E": "0", "F": "0", "G": "1506422161000", "H": "0", "T": "TAGS:tag", "J": "0", "K": "false"}

	request.ExtFields = extFields
	request.Body = []byte("send message test")
	return request, requestHeader
}

func CreateMqtraceContext() *mqtrace.SendMessageContext {
	mqtraceContext := &mqtrace.SendMessageContext{ProducerGroup: "producer", Topic: "TestTopic", MsgId: "",
		OriginMsgId: "", QueueId: 0, QueueOffset: 0,
		BrokerAddr: "10.122.1.17:10911", BornHost: "127.0.0.1:10911",
		BodyLength: 0, Code: 0, ErrorMsg: "", MsgProps: "TAGS:tag",
	}
	return mqtraceContext
}

func CreateConsumerSendMsgBackRequest() *protocol.RemotingCommand {
	requestHeader := header.NewConsumerSendMsgBackRequestHeader()
	requestHeader.Offset = 0
	request := protocol.CreateRequestCommand(commonprotocol.CONSUMER_SEND_MSG_BACK, requestHeader)
	return request
}

package test

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	protocol2 "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"testing"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
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
	request := protocol.CreateRequestCommand(0, nil)
	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	adminProcessor.ProcessRequest(ctx, request)
}

func TestUpdateBrokerConfig(t *testing.T) {
	bc := InitBrokerController()
	ctx := createAdminCtx()
	request := protocol.CreateRequestCommand(0, nil)
	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	adminProcessor.ProcessRequest(ctx, request)
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

package common

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
)

func CreateTopic(bc *stgbroker.BrokerController, ctx netm.Context, topicConfigOld *stgcommon.TopicConfig, newTopic string) (*protocol.RemotingCommand, error) {
	requestHeader := &header.CreateTopicRequestHeader{}
	requestHeader.Topic = newTopic
	requestHeader.DefaultTopic = fmt.Sprintf("%s%d", newTopic, timeutil.CurrentTimeMillis())
	requestHeader.ReadQueueNums = 12
	requestHeader.WriteQueueNums = 12
	requestHeader.Perm = 7
	requestHeader.TopicFilterType = stgcommon.SINGLE_TAG
	requestHeader.Order = false
	requestHeader.TopicSysFlag = 0
	if topicConfigOld != nil {
		requestHeader.TopicSysFlag = topicConfigOld.TopicSysFlag + 1
	}
	return ProcessRequest(bc, ctx, code.UPDATE_AND_CREATE_TOPIC, requestHeader)
}

func ProcessRequest(bc *stgbroker.BrokerController, ctx netm.Context, code int32, requestHeader protocol.CommandCustomHeader) (*protocol.RemotingCommand, error) {
	request := protocol.CreateRequestCommand(code, requestHeader)
	// 调用EncodeHeader方法中的makeCustomHeaderToNet方法:将requestHeader的值写入到ExtFields中
	request.EncodeHeader()
	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	response, err := adminProcessor.ProcessRequest(ctx, request)
	return response, err
}

func CreateAdminCtx() netm.Context {
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

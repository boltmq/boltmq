package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/mqtrace"
	commonprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// AbstractSendMessageProcessor 发送处理类
// Author gaoyanlei
// Since 2017/8/14
type AbstractSendMessageProcessor struct {
	BrokerController *BrokerController
}

// NewAbstractSendMessageProcessor 初始化ConsumerOffsetManager
// Author gaoyanlei
// Since 2017/8/14
func NewAbstractSendMessageProcessor(brokerController *BrokerController) *AbstractSendMessageProcessor {
	return &AbstractSendMessageProcessor{
		BrokerController: brokerController,
	}
}



func (self *AbstractSendMessageProcessor) parseRequestHeader(request protocol.RemotingCommand) *header.SendMessageRequestHeader {
	var requestHeaderV2 *header.SendMessageRequestHeaderV2
	var requestHeader *header.SendMessageRequestHeader
	if request.Code == commonprotocol.SEND_MESSAGE_V2 {
		// TODO  requestHeaderV2 =(SendMessageRequestHeaderV2) request.decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
	}
	if requestHeaderV2 == nil {
		// TODO  requestHeader =(SendMessageRequestHeader) request.decodeCommandCustomHeader(SendMessageRequestHeader.class);
	} else {
		requestHeader = header.CreateSendMessageRequestHeaderV1(requestHeaderV2)
	}
	return requestHeader
}

func (self *AbstractSendMessageProcessor) buildMsgContext( // TODO ChannelHandlerContext ctx
	requestHeader *header.SendMessageRequestHeader) mqtrace.SendMessageContext {
	mqtraceContext := mqtrace.SendMessageContext{}
	mqtraceContext.ProducerGroup = requestHeader.ProducerGroup
	mqtraceContext.Topic = requestHeader.Topic
	mqtraceContext.MsgProps = requestHeader.Properties
	// TODO 	mqtraceContext.BornHost = requestHeader.ProducerGroup
	mqtraceContext.BrokerAddr = self.BrokerController.GetBrokerAddr()
	return mqtraceContext
}
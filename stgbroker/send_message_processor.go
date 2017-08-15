package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/mqtrace"
	commonprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

type SendMessageProcessor struct {
	*AbstractSendMessageProcessor
	BrokerController *BrokerController
}

func (self *SendMessageProcessor) ProcessRequest(request protocol.RemotingCommand, // TODO ChannelHandlerContext ctx
) *protocol.RemotingCommand {

	if request.Code == commonprotocol.CONSUMER_SEND_MSG_BACK {
		return self.consumerSendMsgBack(request)
	}

	requestHeader := self.parseRequestHeader(request)
	if requestHeader == nil {
		return nil
	}
	mqtraceContext := self.buildMsgContext(requestHeader)
	// TODO  this.executeSendMessageHookBefore(ctx, request, mqtraceContext);
	response := self.sendMessage(request, mqtraceContext, requestHeader)
}

func (self *SendMessageProcessor) consumerSendMsgBack(request protocol.RemotingCommand, // TODO ChannelHandlerContext ctx
) (remotingCommand *protocol.RemotingCommand) {

	return
}
func (self *SendMessageProcessor) sendMessage( // TODO final ChannelHandlerContext ctx,
	request protocol.RemotingCommand, context mqtrace.SendMessageContext, header header.SendMessageRequestHeader) protocol.RemotingCommand {
	response := protocol.CreateResponseCommand()
	response.Opaque = request.Opaque
	response.Code = -1
	return nil
}

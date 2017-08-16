package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/mqtrace"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	commonprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
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
	return response
}

func (self *SendMessageProcessor) consumerSendMsgBack(request protocol.RemotingCommand, // TODO ChannelHandlerContext ctx
) (remotingCommand *protocol.RemotingCommand) {

	return
}
func (self *SendMessageProcessor) sendMessage( // TODO final ChannelHandlerContext ctx,
	request protocol.RemotingCommand, mqtraceContext mqtrace.SendMessageContext, requestHeader *header.SendMessageRequestHeader) *protocol.RemotingCommand {
	response := protocol.CreateResponseCommand()
	response.Opaque = request.Opaque
	response.Code = -1
	self.msgCheck(requestHeader, response)
	if response.Code != -1 {
		return response
	}

	body := request.Body

	queueIdInt := requestHeader.QueueId

	topicConfig := self.BrokerController.TopicConfigManager.selectTopicConfig(requestHeader.Topic)

	if queueIdInt < 0 {
		num := (self.Rand.Int() % 99999999) % topicConfig.WriteQueueNums
		if num > 0 {
			queueIdInt = num
		} else {
			queueIdInt = -num
		}

	}

	sysFlag := requestHeader.SysFlag
	if stgcommon.MULTI_TAG == topicConfig.TopicFilterType {
		sysFlag |= sysflag.MultiTagsFlag
	}
	msgInner := new(stgstorelog.MessageExtBrokerInner)
	msgInner.Topic = requestHeader.Topic
	msgInner.Body = body
	//message.MessageAccessor(msgInner,)
	msgInner.Flag = requestHeader.Flag
	msgInner.Flag = requestHeader.Flag
	msgInner.Flag = requestHeader.Flag
	msgInner.Flag = requestHeader.Flag
	return nil
}

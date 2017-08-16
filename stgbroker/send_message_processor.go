package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/mqtrace"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
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
	// TODO  this.executeSendMessageHookBefore(ctx, request, mqtraceContext)
	response := self.sendMessage(request, mqtraceContext, requestHeader)
	return response
}

func (self *SendMessageProcessor) consumerSendMsgBack(request protocol.RemotingCommand, // TODO ChannelHandlerContext ctx
) (remotingCommand *protocol.RemotingCommand) {

	return
}
func (self *SendMessageProcessor) sendMessage( // TODO final ChannelHandlerContext ctx,
	request protocol.RemotingCommand, mqtraceContext mqtrace.SendMessageContext, requestHeader *header.SendMessageRequestHeader) ( *protocol.RemotingCommand) {
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
	message.SetPropertiesMap(msgInner.Message, message.String2messageProperties(requestHeader.Properties))
	msgInner.PropertiesString = requestHeader.Properties
	msgInner.TagsCode = stgstorelog.TagsString2tagsCode(topicConfig.TopicFilterType, msgInner.GetTags())
	msgInner.QueueId = queueIdInt
	msgInner.SysFlag = sysFlag
	msgInner.BornTimestamp = requestHeader.BornTimestamp
	// TODO 	msgInner.BornHost =requestHeader.BornTimestamp
	msgInner.StoreHost = self.StoreHost
	if requestHeader.ReconsumeTimes == 0 {
		msgInner.ReconsumeTimes = 0
	} else {
		msgInner.ReconsumeTimes = requestHeader.ReconsumeTimes
	}

	if self.BrokerController.BrokerConfig.RejectTransactionMessage {
		traFlag := msgInner.GetProperty(message.PROPERTY_TRANSACTION_PREPARED)
		if len(traFlag) > 0 {
			response.Code = commonprotocol.NO_PERMISSION
			response.Remark = "the broker[" + self.BrokerController.BrokerConfig.BrokerIP1 + "] sending transaction message is forbidden"
			return response
		}
	}

	// TODO this.brokerController.getMessageStore().putMessage(msgInner)
	putMessageResult := new(stgstorelog.PutMessageResult)

	if putMessageResult != nil {
		sendOK := false
		switch putMessageResult.PutMessageStatus {
		case stgstorelog.PUTMESSAGE_PUT_OK:
			sendOK = true
			response.Code = commonprotocol.SUCCESS
		case stgstorelog.FLUSH_DISK_TIMEOUT:
			response.Code = commonprotocol.FLUSH_DISK_TIMEOUT
			sendOK = true
			break
		case stgstorelog.FLUSH_SLAVE_TIMEOUT:
			response.Code = commonprotocol.FLUSH_SLAVE_TIMEOUT
			sendOK = true
		case stgstorelog.SLAVE_NOT_AVAILABLE:
			response.Code = commonprotocol.SLAVE_NOT_AVAILABLE
			sendOK = true

		case stgstorelog.CREATE_MAPEDFILE_FAILED:
			response.Code = commonprotocol.SYSTEM_ERROR
			response.Remark = "create maped file failed, please make sure OS and JDK both 64bit."
		case stgstorelog.MESSAGE_ILLEGAL:
			response.Code = commonprotocol.MESSAGE_ILLEGAL
			response.Remark = "the message is illegal, maybe length not matched."
			break
		case stgstorelog.SERVICE_NOT_AVAILABLE:
			response.Code = commonprotocol.SERVICE_NOT_AVAILABLE
			response.Remark = "service not available now, maybe disk full, " + self.diskUtil() + ", maybe your broker machine memory too small."
		case stgstorelog.PUTMESSAGE_UNKNOWN_ERROR:
			response.Code = commonprotocol.SYSTEM_ERROR
			response.Remark = "UNKNOWN_ERROR"
		default:
			response.Code = commonprotocol.SYSTEM_ERROR
			response.Remark = "UNKNOWN_ERROR DEFAULT"
		}

		if sendOK {
			// TODO   this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
			// TODO this.brokerController.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(),
			// TODO 	putMessageResult.getAppendMessageResult().getWroteBytes());
			// TODO this.brokerController.getBrokerStatsManager().incBrokerPutNums();
			response.Remark=nil
		}

	}

	return nil
}

func (self *SendMessageProcessor) diskUtil() string {
	// TODO
	return ""
}

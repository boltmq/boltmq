package stgbroker

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker/mqtrace"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	commonprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"math/rand"
	"strings"
)

const DLQ_NUMS_PER_GROUP  = 1
// AbstractSendMessageProcessor 发送处理类
// Author gaoyanlei
// Since 2017/8/14
type AbstractSendMessageProcessor struct {
	BrokerController *BrokerController
	Rand             *rand.Rand
	StoreHost        string
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

// msgCheck 校验msg
// Author gaoyanlei
// Since 2017/8/16
func (self *AbstractSendMessageProcessor) msgCheck( // TODO ChannelHandlerContext ctx
	requestHeader *header.SendMessageRequestHeader, response *protocol.RemotingCommand) *protocol.RemotingCommand {
	// 如果broker没有写权限，并且topic为顺序topic
	if constant.IsWriteable(self.BrokerController.BrokerConfig.BrokerPermission) &&
		self.BrokerController.TopicConfigManager.IsOrderTopic(requestHeader.Topic) {
		response.Code = commonprotocol.NO_PERMISSION
		response.Remark = "the broker[" + self.BrokerController.BrokerConfig.BrokerIP1 + "] sending message is forbidden"
		return response
	}

	if !self.BrokerController.TopicConfigManager.isTopicCanSendMessage(requestHeader.Topic) {
		response.Code = commonprotocol.SYSTEM_ERROR
		response.Remark = fmt.Sprint("the topic[%s] is conflict with system reserved words.", requestHeader.Topic)
		return response
	}

	topicConfig := self.BrokerController.TopicConfigManager.selectTopicConfig(requestHeader.Topic)
	if topicConfig == nil {
		topicSysFlag := 0
		if requestHeader.UnitMode {
			if strings.Contains(requestHeader.Topic, stgcommon.RETRY_GROUP_TOPIC_PREFIX) {
				topicSysFlag = sysflag.TopicBuildSysFlag(false, true)
			} else {
				topicSysFlag = sysflag.TopicBuildSysFlag(true, false)
			}
		}

		topicConfig, _ := self.BrokerController.TopicConfigManager.createTopicInSendMessageMethod(requestHeader.Topic, requestHeader.DefaultTopic,
			"", requestHeader.DefaultTopicQueueNums, topicSysFlag)
		if topicConfig == nil {
			if strings.Contains(requestHeader.Topic, stgcommon.RETRY_GROUP_TOPIC_PREFIX) {
				topicConfig, _ = self.BrokerController.TopicConfigManager.createTopicInSendMessageBackMethod(requestHeader.Topic,
					1, constant.PERM_WRITE|constant.PERM_READ, topicSysFlag)
			}
		}

		if topicConfig == nil {
			response.Code = commonprotocol.TOPIC_NOT_EXIST
			response.Remark = "topic[" + requestHeader.Topic + "] not exist, apply first please!"
		}

	}

	queueIdInt := requestHeader.QueueId
	idValid := 0
	if topicConfig.WriteQueueNums > topicConfig.ReadQueueNums {
		idValid = topicConfig.WriteQueueNums
	} else {
		idValid = topicConfig.ReadQueueNums
	}

	if queueIdInt >= idValid {
		errorInfo := fmt.Sprintf("request queueId[%d] is illagal, %s producer: %s", //
			queueIdInt,             //
			topicConfig.ToString()) //
		// TODO RemotingHelper.parseChannelRemoteAddr(ctx.channel())
		response.Remark = errorInfo
		response.Code = commonprotocol.SYSTEM_ERROR
		return response
	}
	return response
}

func DoResponse( //TODO ChannelHandlerContext ctx,
	request protocol.RemotingCommand, response *protocol.RemotingCommand) {
	if !request.IsOnewayRPC() {
		// TODO ctx.writeAndFlush(response);
	}
}

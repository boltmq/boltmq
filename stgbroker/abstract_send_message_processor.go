package stgbroker

import (
	"fmt"
	"math/rand"
	"net"
	"strings"

	"git.oschina.net/cloudzone/smartgo/stgbroker/mqtrace"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	commonprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

const DLQ_NUMS_PER_GROUP = 1

// AbstractSendMessageProcessor 发送处理类
// Author gaoyanlei
// Since 2017/8/14
type AbstractSendMessageProcessor struct {
	BrokerController    *BrokerController
	Rand                *rand.Rand
	StoreHost           string
	sendMessageHookList []mqtrace.SendMessageHook
}

// NewAbstractSendMessageProcessor 初始化ConsumerOffsetManager
// Author gaoyanlei
// Since 2017/8/14
func NewAbstractSendMessageProcessor(brokerController *BrokerController) *AbstractSendMessageProcessor {
	return &AbstractSendMessageProcessor{
		BrokerController: brokerController,
		StoreHost:        brokerController.StoreHost,
	}
}

func (asmp *AbstractSendMessageProcessor) parseRequestHeader(request *protocol.RemotingCommand) *header.SendMessageRequestHeader {
	requestHeaderV2 := &header.SendMessageRequestHeaderV2{}
	var requestHeader *header.SendMessageRequestHeader
	if request.Code == commonprotocol.SEND_MESSAGE_V2 {
		err := request.DecodeCommandCustomHeader(requestHeaderV2) // TODO  requestHeaderV2 =(SendMessageRequestHeaderV2) request.decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
		if err != nil {
			fmt.Println("error")
		}
	}

	requestHeader = header.CreateSendMessageRequestHeaderV1(requestHeaderV2)

	return requestHeader
}

func (asmp *AbstractSendMessageProcessor) buildMsgContext(conn net.Conn, requestHeader *header.SendMessageRequestHeader) *mqtrace.SendMessageContext {
	mqtraceContext := &mqtrace.SendMessageContext{}
	mqtraceContext.ProducerGroup = requestHeader.ProducerGroup
	mqtraceContext.Topic = requestHeader.Topic
	mqtraceContext.MsgProps = requestHeader.Properties
	mqtraceContext.BornHost = conn.LocalAddr().String()
	mqtraceContext.BrokerAddr = asmp.BrokerController.GetBrokerAddr()
	return mqtraceContext
}

// msgCheck 校验msg
// Author gaoyanlei
// Since 2017/8/16
func (asmp *AbstractSendMessageProcessor) msgCheck(conn net.Conn, requestHeader *header.SendMessageRequestHeader, response *protocol.RemotingCommand) *protocol.RemotingCommand {
	// 如果broker没有写权限，并且topic为顺序topic
	if constant.IsWriteable(asmp.BrokerController.BrokerConfig.BrokerPermission) &&
		asmp.BrokerController.TopicConfigManager.IsOrderTopic(requestHeader.Topic) {
		response.Code = commonprotocol.NO_PERMISSION
		response.Remark = "the broker[" + asmp.BrokerController.BrokerConfig.BrokerIP1 + "] sending message is forbidden"
		return response
	}

	if !asmp.BrokerController.TopicConfigManager.isTopicCanSendMessage(requestHeader.Topic) {
		response.Code = commonprotocol.SYSTEM_ERROR
		response.Remark = fmt.Sprint("the topic[%s] is conflict with system reserved words.", requestHeader.Topic)
		return response
	}

	topicConfig := asmp.BrokerController.TopicConfigManager.selectTopicConfig(requestHeader.Topic)
	if topicConfig == nil {
		topicSysFlag := 0
		if requestHeader.UnitMode {
			if strings.Contains(requestHeader.Topic, stgcommon.RETRY_GROUP_TOPIC_PREFIX) {
				topicSysFlag = sysflag.TopicBuildSysFlag(false, true)
			} else {
				topicSysFlag = sysflag.TopicBuildSysFlag(true, false)
			}
		}

		topicConfig, _ = asmp.BrokerController.TopicConfigManager.createTopicInSendMessageMethod(requestHeader.Topic, requestHeader.DefaultTopic,
			conn.LocalAddr().String(), requestHeader.DefaultTopicQueueNums, topicSysFlag)
		if topicConfig == nil {
			if strings.Contains(requestHeader.Topic, stgcommon.RETRY_GROUP_TOPIC_PREFIX) {
				topicConfig, _ = asmp.BrokerController.TopicConfigManager.createTopicInSendMessageBackMethod(requestHeader.Topic,
					1, constant.PERM_WRITE|constant.PERM_READ, topicSysFlag)
			}
		}

		if topicConfig == nil {
			response.Code = commonprotocol.TOPIC_NOT_EXIST
			response.Remark = "topic[" + requestHeader.Topic + "] not exist, apply first please!"
		}

	}

	queueIdInt := requestHeader.QueueId
	var idValid int32
	if topicConfig.WriteQueueNums > topicConfig.ReadQueueNums {
		idValid = topicConfig.WriteQueueNums
	} else {
		idValid = topicConfig.ReadQueueNums
	}

	if queueIdInt >= idValid {
		errorInfo := fmt.Sprintf("request queueId[%d] is illagal, %s producer: %s", //
			queueIdInt,             //
			topicConfig.ToString()) //
		conn.LocalAddr().String()
		response.Remark = errorInfo
		response.Code = commonprotocol.SYSTEM_ERROR
		return response
	}
	return response
}

func DoResponse( //TODO ChannelHandlerContext ctx,
	request *protocol.RemotingCommand, response *protocol.RemotingCommand) {
	if !request.IsOnewayRPC() {
		// TODO ctx.writeAndFlush(response);
	}
}

// hasSendMessageHook 检查SendMessageHookList的长度
// Author rongzhihong
// Since 2017/9/11
func (asmp *AbstractSendMessageProcessor) HasSendMessageHook() bool {
	return asmp.sendMessageHookList != nil && len(asmp.sendMessageHookList) > 0
}

// RegisterSendMessageHook 注册赋值
// Author rongzhihong
// Since 2017/9/11
func (asmp *AbstractSendMessageProcessor) RegisterSendMessageHook(sendMessageHookList []mqtrace.SendMessageHook) {
	asmp.sendMessageHookList = sendMessageHookList
}

// ExecuteSendMessageHookBefore 发送消息前执行回调函数
// Author rongzhihong
// Since 2017/9/11
func (asmp *AbstractSendMessageProcessor) ExecuteSendMessageHookBefore(conn net.Conn, request *protocol.RemotingCommand, context *mqtrace.SendMessageContext) {
	defer utils.RecoveredFn()

	if asmp.HasSendMessageHook() {
		for _, hook := range asmp.sendMessageHookList {
			requestHeader := new(header.SendMessageRequestHeader)
			err := request.DecodeCommandCustomHeader(requestHeader)
			if err != nil {
				logger.Error(err)
				continue
			}

			context.ProducerGroup = requestHeader.ProducerGroup
			context.Topic = requestHeader.Topic
			context.BodyLength = len(request.Body)
			context.MsgProps = requestHeader.Properties
			context.BornHost = conn.RemoteAddr().String()
			context.BrokerAddr = asmp.BrokerController.GetBrokerAddr()
			context.QueueId = requestHeader.QueueId

			hook.SendMessageBefore(context)
			requestHeader.Properties = context.MsgProps
		}
	}
}

// ExecuteSendMessageHookAfter 发送消息后执行回调函数
// Author rongzhihong
// Since 2017/9/11
func (asmp *AbstractSendMessageProcessor) ExecuteSendMessageHookAfter(response *protocol.RemotingCommand, context *mqtrace.SendMessageContext) {
	defer utils.RecoveredFn()

	if asmp.HasSendMessageHook() {
		for _, hook := range asmp.sendMessageHookList {
			if response != nil{
				responseHeader := new(header.SendMessageResponseHeader)
				err := response.DecodeCommandCustomHeader(responseHeader)
				if err != nil {
					logger.Error(err)
					continue
				}
				if responseHeader != nil {
					context.MsgId = responseHeader.MsgId
					context.QueueId = responseHeader.QueueId
					context.QueueOffset = responseHeader.QueueOffset
					context.Code = int(response.Code)
					context.ErrorMsg = response.Remark
				}
			}
			hook.SendMessageAfter(context)
		}
	}
}

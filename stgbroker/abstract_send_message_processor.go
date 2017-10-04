package stgbroker

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker/mqtrace"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/remotingUtil"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"math/rand"
	"strings"
)

const (
	DLQ_NUMS_PER_GROUP = 1
)

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

	if request.Code == code.SEND_MESSAGE_V2 {
		err := request.DecodeCommandCustomHeader(requestHeaderV2)
		if err != nil {
			logger.Errorf("error: %s", err.Error())
		}
		requestHeader = header.CreateSendMessageRequestHeaderV1(requestHeaderV2)

	} else if request.Code == code.SEND_MESSAGE {
		requestHeader = &header.SendMessageRequestHeader{}
		err := request.DecodeCommandCustomHeader(requestHeader)
		if err != nil {
			logger.Errorf("error: %s", err.Error())
		}
	}

	return requestHeader
}

func (asmp *AbstractSendMessageProcessor) buildMsgContext(ctx netm.Context, requestHeader *header.SendMessageRequestHeader) *mqtrace.SendMessageContext {
	mqtraceContext := &mqtrace.SendMessageContext{}
	mqtraceContext.ProducerGroup = requestHeader.ProducerGroup
	mqtraceContext.Topic = requestHeader.Topic
	mqtraceContext.MsgProps = requestHeader.Properties
	mqtraceContext.BornHost = ctx.LocalAddr().String()
	mqtraceContext.BrokerAddr = asmp.BrokerController.GetBrokerAddr()
	return mqtraceContext
}

// msgCheck 校验msg
// Author gaoyanlei
// Since 2017/8/16
func (asmp *AbstractSendMessageProcessor) msgCheck(ctx netm.Context, requestHeader *header.SendMessageRequestHeader, response *protocol.RemotingCommand) *protocol.RemotingCommand {
	// 如果broker没有写权限，并且topic为顺序topic
	if !asmp.BrokerController.BrokerConfig.HasWriteable() && asmp.BrokerController.TopicConfigManager.IsOrderTopic(requestHeader.Topic) {
		response.Code = code.NO_PERMISSION
		response.Remark = fmt.Sprintf("the broker[%s] sending message is forbidden", asmp.BrokerController.BrokerConfig.BrokerIP1)
		return response
	}

	if !asmp.BrokerController.TopicConfigManager.isTopicCanSendMessage(requestHeader.Topic) {
		response.Code = code.SYSTEM_ERROR
		response.Remark = fmt.Sprintf("the topic[%s] is conflict with system reserved words.", requestHeader.Topic)
		return response
	}

	topicConfig := asmp.BrokerController.TopicConfigManager.SelectTopicConfig(requestHeader.Topic)
	if topicConfig == nil {
		topicSysFlag := 0
		if requestHeader.UnitMode {
			topicSysFlag = sysflag.TopicBuildSysFlag(true, false)
			if strings.Contains(requestHeader.Topic, stgcommon.RETRY_GROUP_TOPIC_PREFIX) {
				topicSysFlag = sysflag.TopicBuildSysFlag(false, true)
			}
		}

		topicConfig, _ = asmp.BrokerController.TopicConfigManager.CreateTopicInSendMessageMethod(
			requestHeader.Topic,                 // 1
			requestHeader.DefaultTopic,          // 2
			ctx.LocalAddr().String(),            // 3
			requestHeader.DefaultTopicQueueNums, // 4
			topicSysFlag,                        // 5
		)

		if topicConfig == nil {
			if strings.Contains(requestHeader.Topic, stgcommon.RETRY_GROUP_TOPIC_PREFIX) {
				permNum := constant.PERM_WRITE | constant.PERM_READ
				topicConfig, _ = asmp.BrokerController.TopicConfigManager.CreateTopicInSendMessageBackMethod(
					requestHeader.Topic, // 1
					1,                   // 2
					permNum,             // 3
					topicSysFlag,        // 4
				)
			}
		}

		if topicConfig == nil {
			response.Code = code.TOPIC_NOT_EXIST
			response.Remark = fmt.Sprintf("topic[%s] not exist, apply first please!", requestHeader.Topic)
			return response
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
		format := "request queueId[%d] is illagal, %s producer: %s"
		errorInfo := fmt.Sprintf(format, queueIdInt, topicConfig.ToString(), remotingUtil.ParseChannelRemoteAddr(ctx))

		logger.Warn(errorInfo)
		response.Remark = errorInfo
		response.Code = code.SYSTEM_ERROR
		return response
	}
	return response
}

func DoResponse(ctx netm.Context,
	request *protocol.RemotingCommand, response *protocol.RemotingCommand) {
	if !request.IsOnewayRPC() {
		_, err := ctx.WriteSerialObject(response)
		if err != nil {
			logger.Errorf("DoResponse:%s", err.Error())
			return
		}
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
func (asmp *AbstractSendMessageProcessor) ExecuteSendMessageHookBefore(ctx netm.Context, request *protocol.RemotingCommand, context *mqtrace.SendMessageContext) {
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
			context.BornHost = ctx.RemoteAddr().String()
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
			if response != nil {
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

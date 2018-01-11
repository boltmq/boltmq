// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package server

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/boltmq/boltmq/broker/trace"
	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/boltmq/net/core"
	"github.com/boltmq/boltmq/store"
	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/constant"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/head"
	"github.com/boltmq/common/sysflag"
)

const (
	DLQ_NUMS_PER_GROUP = 1
)

// SendMessageProcessor 处理客户端发送消息的请求
// Author gaoyanlei
// Since 2017/8/24
type SendMessageProcessor struct {
	basicSendMsgProcessor  *basicSendMessageProcessor
	brokerController       *BrokerController
	sendMessageHookList    []trace.SendMessageHook
	consumeMessageHookList []trace.ConsumeMessageHook
}

func NewSendMessageProcessor(brokerController *BrokerController) *SendMessageProcessor {
	var smp = new(SendMessageProcessor)
	smp.brokerController = brokerController
	smp.basicSendMsgProcessor = newBasicSendMessageProcessor(brokerController)
	return smp
}

func (smp *SendMessageProcessor) ProcessRequest(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {

	if request.Code == protocol.CONSUMER_SEND_MSG_BACK {
		return smp.ConsumerSendMsgBack(ctx, request), nil
	}

	requestHeader := smp.basicSendMsgProcessor.parseRequestHeader(request)
	if requestHeader == nil {
		return nil, nil
	}

	traceContext := smp.basicSendMsgProcessor.buildMsgContext(ctx, requestHeader)
	smp.basicSendMsgProcessor.ExecuteSendMessageHookBefore(ctx, request, traceContext)
	response := smp.SendMessage(ctx, request, traceContext, requestHeader)
	smp.basicSendMsgProcessor.ExecuteSendMessageHookAfter(response, traceContext)
	return response, nil
}

// consumerSendMsgBack 客户端返回未消费消息
// Author gaoyanlei
// Since 2017/8/17
func (smp *SendMessageProcessor) ConsumerSendMsgBack(conn core.Context,
	request *protocol.RemotingCommand) (remotingCommand *protocol.RemotingCommand) {
	response := &protocol.RemotingCommand{}

	requestHeader := head.NewConsumerSendMsgBackRequestHeader()
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("consumer send msg back err: %s.", err)
	}

	// 消息轨迹：记录消费失败的消息
	if len(requestHeader.OriginMsgId) > 0 {
		context := new(trace.ConsumeMessageContext)
		context.ConsumerGroup = requestHeader.Group
		context.Topic = requestHeader.OriginTopic
		context.ClientHost = conn.RemoteAddr().String()
		context.Success = false
		context.Status = string(protocol.RECONSUME_LATER)
		messageIds := make(map[string]int64)
		messageIds[requestHeader.OriginMsgId] = requestHeader.Offset
		context.MessageIds = messageIds
		smp.ExecuteConsumeMessageHookAfter(context)
	}

	// 确保订阅组存在
	subscriptionGroupConfig := smp.brokerController.subGroupManager.findSubscriptionGroupConfig(requestHeader.Group)
	if subscriptionGroupConfig == nil {
		response.Code = protocol.SUBSCRIPTION_GROUP_NOT_EXIST
		response.Remark = "subscription group not exist"
		return response
	}

	// 检查Broker权限
	if !smp.brokerController.cfg.HasWriteable() {
		response.Code = protocol.NO_PERMISSION
		response.Remark = "the broker[" + smp.brokerController.cfg.Broker.IP + "] sending message is forbidden"
		return response
	}

	// 如果重试队列数目为0，则直接丢弃消息
	retryQueueNums := subscriptionGroupConfig.RetryQueueNums
	if retryQueueNums <= 0 {
		response.Code = protocol.SUCCESS
		response.Remark = ""
		return response
	}

	newTopic := getRetryTopic(requestHeader.Group)
	var queueIdInt int32
	if queueIdInt < 0 {
		num := (smp.basicSendMsgProcessor.random.Int31() % 99999999) % retryQueueNums
		if num > 0 {
			queueIdInt = num
		} else {
			queueIdInt = -num
		}
	}

	// 如果是单元化模式，则对 topic 进行设置
	topicSysFlag := 0
	if requestHeader.UnitMode {
		topicSysFlag = sysflag.TopicBuildSysFlag(false, true)
	}

	// 检查topic是否存在
	perm := constant.PERM_WRITE | constant.PERM_READ
	topicConfig, err := smp.brokerController.tpConfigManager.createTopicInSendMessageBackMethod(newTopic, retryQueueNums, perm, topicSysFlag)
	if topicConfig == nil || err != nil {
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = "topic[" + newTopic + "] not exist"
		return response
	}

	// 检查topic权限
	if !constant.IsWriteable(topicConfig.Perm) {
		response.Code = protocol.NO_PERMISSION
		response.Remark = "the topic[" + newTopic + "] sending message is forbidden"
		return response
	}

	// 查询消息，这里如果堆积消息过多，会访问磁盘
	// 另外如果频繁调用，是否会引起gc问题，需要关注
	msgExt := smp.brokerController.messageStore.LookMessageByOffset(requestHeader.Offset)
	if nil == msgExt {
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = "look message by offset failed, " + string(requestHeader.Offset)
		return response
	}

	// 构造消息
	retryTopic := msgExt.GetProperty(message.PROPERTY_RETRY_TOPIC)
	if "" == retryTopic {
		message.PutProperty(&msgExt.Message, message.PROPERTY_RETRY_TOPIC, msgExt.Topic)
	}
	msgExt.SetWaitStoreMsgOK(false)

	// 客户端自动决定定时级别
	delayLevel := requestHeader.DelayLevel

	// 死信消息处理
	if msgExt.ReconsumeTimes >= subscriptionGroupConfig.RetryMaxTimes || delayLevel < 0 {
		newTopic = getDLQTopic(requestHeader.Group)
		if queueIdInt < 0 {
			num := (smp.basicSendMsgProcessor.random.Int31() % 99999999) % DLQ_NUMS_PER_GROUP
			if num > 0 {
				queueIdInt = num
			} else {
				queueIdInt = -num
			}
		}

		topicConfig, err = smp.brokerController.tpConfigManager.createTopicInSendMessageBackMethod(newTopic, DLQ_NUMS_PER_GROUP, constant.PERM_WRITE, 0)
		if nil == topicConfig {
			response.Code = protocol.SYSTEM_ERROR
			response.Remark = fmt.Sprintf("topic[%s] not exist", newTopic)
			return response
		}
	} else {
		if 0 == delayLevel {
			delayLevel = 3 + msgExt.ReconsumeTimes
		}

		msgExt.SetDelayTimeLevel(int(delayLevel))
	}

	msgInner := new(store.MessageExtInner)
	msgInner.Topic = newTopic
	msgInner.Body = msgExt.Body
	msgInner.Flag = msgExt.Flag
	message.SetPropertiesMap(&msgInner.Message, msgExt.Properties)
	msgInner.PropertiesString = message.MessageProperties2String(msgExt.Properties)
	msgInner.TagsCode = basis.TagsString2tagsCode(basis.SINGLE_TAG, msgExt.GetTags())

	msgInner.QueueId = int32(queueIdInt)
	msgInner.SysFlag = msgExt.SysFlag
	msgInner.BornTimestamp = msgExt.BornTimestamp
	msgInner.BornHost = msgExt.BornHost
	msgInner.StoreHost = smp.brokerController.getStoreHost()
	msgInner.ReconsumeTimes = msgExt.ReconsumeTimes + 1

	// 保存源生消息的 msgId
	originMsgId := message.GetOriginMessageId(msgExt.Message)
	if originMsgId == "" || len(originMsgId) <= 0 {
		originMsgId = msgExt.MsgId

	}
	message.SetOriginMessageId(&msgInner.Message, originMsgId)

	putMessageResult := smp.brokerController.messageStore.PutMessage(msgInner)
	if putMessageResult != nil {
		switch putMessageResult.Status {
		case store.PUTMESSAGE_PUT_OK:
			backTopic := msgExt.Topic
			correctTopic := msgExt.GetProperty(message.PROPERTY_RETRY_TOPIC)
			if correctTopic != "" && len(correctTopic) > 0 {
				backTopic = correctTopic
			}

			smp.brokerController.brokerStats.IncSendBackNums(requestHeader.Group, backTopic)

			response.Code = protocol.SUCCESS
			response.Remark = ""

			return response
		default:
			break
		}
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = putMessageResult.Status.String()
		return response
	}
	response.Code = protocol.SYSTEM_ERROR
	response.Remark = "putMessageResult is null"
	return response
}

// sendMessage 正常消息
// Author gaoyanlei
// Since 2017/8/17
func (smp *SendMessageProcessor) SendMessage(ctx core.Context, request *protocol.RemotingCommand,
	traceContext *trace.SendMessageContext, requestHeader *head.SendMessageRequestHeader) *protocol.RemotingCommand {
	responseHeader := new(head.SendMessageResponseHeader)
	response := protocol.CreateDefaultResponseCommand(responseHeader)
	response.Opaque = request.Opaque
	response.Code = -1
	smp.basicSendMsgProcessor.msgCheck(ctx, requestHeader, response)
	if response.Code != -1 {
		return response
	}

	body := request.Body

	queueIdInt := requestHeader.QueueId

	topicConfig := smp.brokerController.tpConfigManager.selectTopicConfig(requestHeader.Topic)

	if queueIdInt < 0 {
		num := (smp.basicSendMsgProcessor.random.Int31() % 99999999) % topicConfig.WriteQueueNums
		if num > 0 {
			queueIdInt = int32(num)
		} else {
			queueIdInt = -int32(num)
		}

	}

	sysFlag := requestHeader.SysFlag
	if basis.MULTI_TAG == topicConfig.TpFilterType {
		sysFlag |= sysflag.MultiTagsFlag
	}
	msgInner := new(store.MessageExtInner)
	msgInner.Topic = requestHeader.Topic
	msgInner.Body = body
	msgInner.Flag = requestHeader.Flag
	message.SetPropertiesMap(&msgInner.Message, message.String2messageProperties(requestHeader.Properties))
	msgInner.PropertiesString = requestHeader.Properties
	msgInner.TagsCode = basis.TagsString2tagsCode(topicConfig.TpFilterType, msgInner.GetTags())
	msgInner.QueueId = queueIdInt
	msgInner.SysFlag = sysFlag
	msgInner.BornTimestamp = requestHeader.BornTimestamp
	msgInner.BornHost = ctx.RemoteAddr().String()
	msgInner.StoreHost = smp.brokerController.getStoreHost()
	if requestHeader.ReconsumeTimes == 0 {
		msgInner.ReconsumeTimes = 0
	} else {
		msgInner.ReconsumeTimes = requestHeader.ReconsumeTimes
	}

	if smp.brokerController.cfg.Broker.RejectTransactionMessage {
		traFlag := msgInner.GetProperty(message.PROPERTY_TRANSACTION_PREPARED)
		if len(traFlag) > 0 {
			response.Code = protocol.NO_PERMISSION
			response.Remark = "the broker[" + smp.brokerController.cfg.Broker.IP + "] sending transaction message is forbidden"
			return response
		}
	}

	putMessageResult := smp.brokerController.messageStore.PutMessage(msgInner)
	if putMessageResult != nil {
		sendOK := false
		switch putMessageResult.Status {
		case store.PUTMESSAGE_PUT_OK:
			sendOK = true
			response.Code = protocol.SUCCESS
		case store.FLUSH_DISK_TIMEOUT:
			response.Code = protocol.FLUSH_DISK_TIMEOUT
			sendOK = true
		case store.FLUSH_SLAVE_TIMEOUT:
			response.Code = protocol.FLUSH_SLAVE_TIMEOUT
			sendOK = true
		case store.SLAVE_NOT_AVAILABLE:
			response.Code = protocol.SLAVE_NOT_AVAILABLE
			sendOK = true

		case store.CREATE_MAPPED_FILE_FAILED:
			response.Code = protocol.SYSTEM_ERROR
			response.Remark = "create maped file failed, please make sure OS and JDK both 64bit."
		case store.MESSAGE_ILLEGAL:
			response.Code = protocol.MESSAGE_ILLEGAL
			response.Remark = "the message is illegal, maybe length not matched."
		case store.SERVICE_NOT_AVAILABLE:
			response.Code = protocol.SERVICE_NOT_AVAILABLE
			response.Remark = "service not available now, maybe disk full, " + smp.diskUtil() + ", maybe your broker machine memory too small."
		case store.PUTMESSAGE_UNKNOWN_ERROR:
			response.Code = protocol.SYSTEM_ERROR
			response.Remark = "UNKNOWN_ERROR"
		default:
			response.Code = protocol.SYSTEM_ERROR
			response.Remark = "UNKNOWN_ERROR DEFAULT"
		}

		if sendOK {
			smp.brokerController.brokerStats.IncTopicPutNums(msgInner.Topic)
			smp.brokerController.brokerStats.IncTopicPutSize(msgInner.Topic, putMessageResult.Result.WroteBytes)
			smp.brokerController.brokerStats.IncBrokerPutNums()

			response.Remark = ""
			responseHeader.MsgId = putMessageResult.Result.MsgId
			responseHeader.QueueId = queueIdInt
			responseHeader.QueueOffset = putMessageResult.Result.LogicsOffset

			DoResponse(ctx, request, response)
			if smp.brokerController.cfg.Broker.LongPollingEnable {
				smp.brokerController.pullRequestHoldSrv.notifyMessageArriving(
					requestHeader.Topic, queueIdInt, putMessageResult.Result.LogicsOffset+1)
			}

			// 消息轨迹：记录发送成功的消息
			if smp.HasSendMessageHook() {
				traceContext.MsgId = responseHeader.MsgId
				traceContext.QueueId = responseHeader.QueueId
				traceContext.QueueOffset = responseHeader.QueueOffset
			}
			return nil
		}

	} else {
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = "store putMessage return null"
	}

	return response
}

// HasSendMessageHook 判断是否存在发送消息回调
// Author rongzhihong
// Since 2017/9/5
func (smp *SendMessageProcessor) HasSendMessageHook() bool {
	return smp.sendMessageHookList != nil && len(smp.sendMessageHookList) > 0
}

// RegisterSendMessageHook 注册赋值发送消息回调
// Author rongzhihong
// Since 2017/9/5
func (smp *SendMessageProcessor) RegisterSendMessageHook(sendMessageHookList []trace.SendMessageHook) {
	smp.sendMessageHookList = sendMessageHookList
}

// HasConsumeMessageHook 判断是否存在消费消息回调
// Author rongzhihong
// Since 2017/9/5
func (smp *SendMessageProcessor) HasConsumeMessageHook() bool {
	return smp.consumeMessageHookList != nil && len(smp.consumeMessageHookList) > 0
}

// RegisterSendMessageHook 注册赋值消费消息回调
// Author rongzhihong
// Since 2017/9/5
func (smp *SendMessageProcessor) RegisterConsumeMessageHook(consumeMessageHookList []trace.ConsumeMessageHook) {
	smp.consumeMessageHookList = consumeMessageHookList
}

// ExecuteConsumeMessageHookAfter 消费消息后执行回调
// Author rongzhihong
// Since 2017/9/5
func (smp *SendMessageProcessor) ExecuteConsumeMessageHookAfter(context *trace.ConsumeMessageContext) {
	if smp.HasConsumeMessageHook() {
		for _, hook := range smp.consumeMessageHookList {
			hook.ConsumeMessageAfter(context)
		}
	}
}

// diskUtil 磁盘使用情况
// Author rongzhihong
// Since 2017/9/16
func (smp *SendMessageProcessor) diskUtil() string {
	storePathPhysic := smp.brokerController.storeCfg.StorePathCommitLog

	physicRatio := common.GetDiskPartitionSpaceUsedPercent(storePathPhysic)

	storePathLogis := common.GetStorePathConsumeQueue(smp.brokerController.storeCfg.StorePathRootDir)

	logisRatio := common.GetDiskPartitionSpaceUsedPercent(storePathLogis)

	storePathIndex := common.GetStorePathIndex(smp.brokerController.storeCfg.StorePathRootDir)

	indexRatio := common.GetDiskPartitionSpaceUsedPercent(storePathIndex)

	return fmt.Sprintf("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio)
}

// basicSendMessageProcessor 发送处理类
// Author gaoyanlei
// Since 2017/8/14
type basicSendMessageProcessor struct {
	brokerController    *BrokerController
	random              *rand.Rand
	sendMessageHookList []trace.SendMessageHook
}

// newBasicSendMessageProcessor 初始化ConsumerOffsetManager
// Author gaoyanlei
// Since 2017/8/14
func newBasicSendMessageProcessor(brokerController *BrokerController) *basicSendMessageProcessor {
	return &basicSendMessageProcessor{
		brokerController: brokerController,
		random:           rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (bsmp *basicSendMessageProcessor) parseRequestHeader(request *protocol.RemotingCommand) *head.SendMessageRequestHeader {
	requestHeaderV2 := &head.SendMessageRequestHeaderV2{}

	var requestHeader *head.SendMessageRequestHeader

	if request.Code == protocol.SEND_MESSAGE_V2 {
		err := request.DecodeCommandCustomHeader(requestHeaderV2)
		if err != nil {
			logger.Errorf("parse request header err: %s.", err)
		}
		requestHeader = head.CreateSendMessageRequestHeaderV1(requestHeaderV2)

	} else if request.Code == protocol.SEND_MESSAGE {
		requestHeader = &head.SendMessageRequestHeader{}
		err := request.DecodeCommandCustomHeader(requestHeader)
		if err != nil {
			logger.Errorf("parse request header err: %s.", err)
		}
	}

	return requestHeader
}

func (bsmp *basicSendMessageProcessor) buildMsgContext(ctx core.Context, requestHeader *head.SendMessageRequestHeader) *trace.SendMessageContext {
	traceContext := &trace.SendMessageContext{}
	traceContext.ProducerGroup = requestHeader.ProducerGroup
	traceContext.Topic = requestHeader.Topic
	traceContext.MsgProps = requestHeader.Properties
	traceContext.BornHost = ctx.LocalAddr().String()
	traceContext.BrokerAddr = bsmp.brokerController.getBrokerAddr()
	return traceContext
}

// msgCheck 校验msg
// Author gaoyanlei
// Since 2017/8/16
func (bsmp *basicSendMessageProcessor) msgCheck(ctx core.Context, requestHeader *head.SendMessageRequestHeader, response *protocol.RemotingCommand) *protocol.RemotingCommand {
	// 如果broker没有写权限，并且topic为顺序topic
	if !bsmp.brokerController.cfg.HasWriteable() && bsmp.brokerController.tpConfigManager.isOrderTopic(requestHeader.Topic) {
		response.Code = protocol.NO_PERMISSION
		response.Remark = fmt.Sprintf("the broker[%s] sending message is forbidden", bsmp.brokerController.cfg.Broker.IP)
		return response
	}

	if !bsmp.brokerController.tpConfigManager.isTopicCanSendMessage(requestHeader.Topic) {
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = fmt.Sprintf("the topic[%s] is conflict with system reserved words.", requestHeader.Topic)
		return response
	}

	topicConfig := bsmp.brokerController.tpConfigManager.selectTopicConfig(requestHeader.Topic)
	if topicConfig == nil {
		topicSysFlag := 0
		if requestHeader.UnitMode {
			if strings.Contains(requestHeader.Topic, basis.RETRY_GROUP_TOPIC_PREFIX) {
				topicSysFlag = sysflag.TopicBuildSysFlag(false, true)
			} else {
				topicSysFlag = sysflag.TopicBuildSysFlag(true, false)
			}
		}

		topicConfig, _ = bsmp.brokerController.tpConfigManager.createTopicInSendMessageMethod(
			requestHeader.Topic,                 // 1
			requestHeader.DefaultTopic,          // 2
			ctx.UniqueSocketAddr().String(),     // 3
			requestHeader.DefaultTopicQueueNums, // 4
			topicSysFlag,                        // 5
		)

		if topicConfig == nil {
			if strings.Contains(requestHeader.Topic, basis.RETRY_GROUP_TOPIC_PREFIX) {
				permNum := constant.PERM_WRITE | constant.PERM_READ
				topicConfig, _ = bsmp.brokerController.tpConfigManager.createTopicInSendMessageBackMethod(
					requestHeader.Topic, // 1
					1,                   // 2
					permNum,             // 3
					topicSysFlag,        // 4
				)
			}
		}

		if topicConfig == nil {
			response.Code = protocol.TOPIC_NOT_EXIST
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
		errorInfo := fmt.Sprintf(format, queueIdInt, topicConfig, parseChannelRemoteAddr(ctx))

		logger.Warn("msg check err: %s.", errorInfo)
		response.Remark = errorInfo
		response.Code = protocol.SYSTEM_ERROR
		return response
	}
	return response
}

func DoResponse(ctx core.Context,
	request *protocol.RemotingCommand, response *protocol.RemotingCommand) {
	if !request.IsOnewayRPC() {
		_, err := ctx.WriteSerialData(response)
		if err != nil {
			logger.Errorf("send message processor request over, but response failed: %s.", err)
			return
		}
	}
}

// hasSendMessageHook 检查SendMessageHookList的长度
// Author rongzhihong
// Since 2017/9/11
func (bsmp *basicSendMessageProcessor) HasSendMessageHook() bool {
	return bsmp.sendMessageHookList != nil && len(bsmp.sendMessageHookList) > 0
}

// RegisterSendMessageHook 注册赋值
// Author rongzhihong
// Since 2017/9/11
func (bsmp *basicSendMessageProcessor) RegisterSendMessageHook(sendMessageHookList []trace.SendMessageHook) {
	bsmp.sendMessageHookList = sendMessageHookList
}

// ExecuteSendMessageHookBefore 发送消息前执行回调函数
// Author rongzhihong
// Since 2017/9/11
func (bsmp *basicSendMessageProcessor) ExecuteSendMessageHookBefore(ctx core.Context, request *protocol.RemotingCommand, context *trace.SendMessageContext) {
	if bsmp.HasSendMessageHook() {
		for _, hook := range bsmp.sendMessageHookList {
			requestHeader := new(head.SendMessageRequestHeader)
			err := request.DecodeCommandCustomHeader(requestHeader)
			if err != nil {
				logger.Errorf("execute send message hook before err: %s.", err)
				continue
			}

			context.ProducerGroup = requestHeader.ProducerGroup
			context.Topic = requestHeader.Topic
			context.BodyLength = len(request.Body)
			context.MsgProps = requestHeader.Properties
			context.BornHost = ctx.RemoteAddr().String()
			context.BrokerAddr = bsmp.brokerController.getBrokerAddr()
			context.QueueId = requestHeader.QueueId

			hook.SendMessageBefore(context)
			requestHeader.Properties = context.MsgProps
		}
	}
}

// ExecuteSendMessageHookAfter 发送消息后执行回调函数
// Author rongzhihong
// Since 2017/9/11
func (bsmp *basicSendMessageProcessor) ExecuteSendMessageHookAfter(response *protocol.RemotingCommand, context *trace.SendMessageContext) {
	if bsmp.HasSendMessageHook() {
		for _, hook := range bsmp.sendMessageHookList {
			if response != nil {
				responseHeader := new(head.SendMessageResponseHeader)
				err := response.DecodeCommandCustomHeader(responseHeader)
				if err != nil {
					logger.Errorf("execute send message hook after err: %s.", err)
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

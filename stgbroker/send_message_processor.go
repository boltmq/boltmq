package stgbroker

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker/mqtrace"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/listener"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
)

// SendMessageProcessor 处理客户端发送消息的请求
// Author gaoyanlei
// Since 2017/8/24
type SendMessageProcessor struct {
	abstractSendMessageProcessor *AbstractSendMessageProcessor
	BrokerController             *BrokerController
	sendMessageHookList          []mqtrace.SendMessageHook
	consumeMessageHookList       []mqtrace.ConsumeMessageHook
}

func NewSendMessageProcessor(brokerController *BrokerController) *SendMessageProcessor {
	var sendMessageProcessor = new(SendMessageProcessor)
	sendMessageProcessor.BrokerController = brokerController
	sendMessageProcessor.abstractSendMessageProcessor = NewAbstractSendMessageProcessor(brokerController)
	return sendMessageProcessor
}

func (smp *SendMessageProcessor) ProcessRequest(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {

	if request.Code == code.CONSUMER_SEND_MSG_BACK {
		return smp.consumerSendMsgBack(ctx, request), nil
	}

	requestHeader := smp.abstractSendMessageProcessor.parseRequestHeader(request)
	if requestHeader == nil {
		return nil, nil
	}

	mqtraceContext := smp.abstractSendMessageProcessor.buildMsgContext(ctx, requestHeader)
	smp.abstractSendMessageProcessor.ExecuteSendMessageHookBefore(ctx, request, mqtraceContext)
	response := smp.sendMessage(ctx, request, mqtraceContext, requestHeader)
	smp.abstractSendMessageProcessor.ExecuteSendMessageHookAfter(response, mqtraceContext)
	return response, nil
}

// consumerSendMsgBack 客户端返回未消费消息
// Author gaoyanlei
// Since 2017/8/17
func (smp *SendMessageProcessor) consumerSendMsgBack(conn netm.Context,
	request *protocol.RemotingCommand) (remotingCommand *protocol.RemotingCommand) {
	response := &protocol.RemotingCommand{}

	requestHeader := header.NewConsumerSendMsgBackRequestHeader()
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	// 消息轨迹：记录消费失败的消息
	if len(requestHeader.OriginMsgId) > 0 {
		context := new(mqtrace.ConsumeMessageContext)
		context.ConsumerGroup = requestHeader.Group
		context.Topic = requestHeader.OriginTopic
		context.ClientHost = conn.RemoteAddr().String()
		context.Success = false
		context.Status = string(listener.RECONSUME_LATER)
		messageIds := make(map[string]int64)
		messageIds[requestHeader.OriginMsgId] = requestHeader.Offset
		context.MessageIds = messageIds
		smp.ExecuteConsumeMessageHookAfter(context)
	}

	// 确保订阅组存在
	subscriptionGroupConfig := smp.BrokerController.SubscriptionGroupManager.FindSubscriptionGroupConfig(requestHeader.Group)
	if subscriptionGroupConfig == nil {
		response.Code = code.SUBSCRIPTION_GROUP_NOT_EXIST
		response.Remark = "subscription group not exist"
		return response
	}

	// 检查Broker权限
	if !constant.IsWriteable(smp.BrokerController.BrokerConfig.BrokerPermission) {
		response.Code = code.NO_PERMISSION
		response.Remark = "the broker[" + smp.BrokerController.BrokerConfig.BrokerIP1 + "] sending message is forbidden"
		return response
	}

	// 如果重试队列数目为0，则直接丢弃消息
	if subscriptionGroupConfig.RetryQueueNums <= 0 {
		response.Code = code.SUCCESS
		response.Remark = ""
		return response
	}

	newTopic := stgcommon.GetRetryTopic(requestHeader.Group)
	var queueIdInt int32
	if queueIdInt < 0 {
		num := (smp.abstractSendMessageProcessor.Rand.Int31() % 99999999) % subscriptionGroupConfig.RetryQueueNums
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
	topicConfig, err := smp.BrokerController.TopicConfigManager.CreateTopicInSendMessageBackMethod(newTopic, subscriptionGroupConfig.RetryQueueNums,
		constant.PERM_WRITE|constant.PERM_READ, topicSysFlag)
	if topicConfig == nil || err != nil {
		response.Code = code.SYSTEM_ERROR
		response.Remark = "topic[" + newTopic + "] not exist"
		return response
	}

	// 检查topic权限
	if !constant.IsWriteable(topicConfig.Perm) {
		response.Code = code.NO_PERMISSION
		response.Remark = "the topic[" + newTopic + "] sending message is forbidden"
		return response
	}

	// 查询消息，这里如果堆积消息过多，会访问磁盘
	// 另外如果频繁调用，是否会引起gc问题，需要关注
	msgExt := smp.BrokerController.MessageStore.LookMessageByOffset(requestHeader.Offset)
	if nil == msgExt {
		response.Code = code.SYSTEM_ERROR
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
		newTopic = stgcommon.GetDLQTopic(requestHeader.Group)
		if queueIdInt < 0 {
			num := (smp.abstractSendMessageProcessor.Rand.Int31() % 99999999) % DLQ_NUMS_PER_GROUP
			if num > 0 {
				queueIdInt = num
			} else {
				queueIdInt = -num
			}
		}

		topicConfig, err =
			smp.BrokerController.TopicConfigManager.CreateTopicInSendMessageBackMethod(
				newTopic, DLQ_NUMS_PER_GROUP, constant.PERM_WRITE, 0)
		if nil == topicConfig {
			response.Code = code.SYSTEM_ERROR
			response.Remark = "topic[" + newTopic + "] not exist"
			return response
		}
	} else {
		if 0 == delayLevel {
			delayLevel = 3 + msgExt.ReconsumeTimes
		}

		msgExt.SetDelayTimeLevel(int(delayLevel))
	}

	msgInner := new(stgstorelog.MessageExtBrokerInner)
	msgInner.Topic = newTopic
	msgInner.Body = msgExt.Body
	msgInner.Flag = msgExt.Flag
	message.SetPropertiesMap(&msgInner.Message, msgExt.Properties)
	msgInner.PropertiesString = message.MessageProperties2String(msgExt.Properties)
	msgInner.TagsCode = stgstorelog.TagsString2tagsCode(stgcommon.SINGLE_TAG, msgExt.GetTags())

	msgInner.QueueId = int32(queueIdInt)
	msgInner.SysFlag = msgExt.SysFlag
	msgInner.BornTimestamp = msgExt.BornTimestamp
	msgInner.BornHost = msgExt.BornHost
	msgInner.StoreHost = smp.abstractSendMessageProcessor.StoreHost
	msgInner.ReconsumeTimes = msgExt.ReconsumeTimes + 1

	// 保存源生消息的 msgId
	originMsgId := message.GetOriginMessageId(msgExt.Message)
	if originMsgId == "" || len(originMsgId) <= 0 {
		originMsgId = msgExt.MsgId

	}
	message.SetOriginMessageId(&msgInner.Message, originMsgId)

	putMessageResult := smp.BrokerController.MessageStore.PutMessage(msgInner)
	if putMessageResult != nil {
		switch putMessageResult.PutMessageStatus {
		case stgstorelog.PUTMESSAGE_PUT_OK:
			backTopic := msgExt.Topic
			correctTopic := msgExt.GetProperty(message.PROPERTY_RETRY_TOPIC)
			if correctTopic == "" || len(correctTopic) <= 0 {
				backTopic = correctTopic
			}

			smp.BrokerController.brokerStatsManager.IncSendBackNums(requestHeader.Group, backTopic)

			response.Code = code.SUCCESS
			response.Remark = ""

			return response
		default:
			break
		}
		response.Code = code.SYSTEM_ERROR
		response.Remark = putMessageResult.PutMessageStatus.PutMessageString()
		return response
	}
	response.Code = code.SYSTEM_ERROR
	response.Remark = "putMessageResult is null"
	return response
}

// sendMessage 正常消息
// Author gaoyanlei
// Since 2017/8/17
func (smp *SendMessageProcessor) sendMessage(ctx netm.Context, request *protocol.RemotingCommand,
	mqtraceContext *mqtrace.SendMessageContext, requestHeader *header.SendMessageRequestHeader) *protocol.RemotingCommand {
	responseHeader := new(header.SendMessageResponseHeader)
	response := protocol.CreateDefaultResponseCommand(responseHeader)
	response.Opaque = request.Opaque
	response.Code = -1
	smp.abstractSendMessageProcessor.msgCheck(ctx, requestHeader, response)
	if response.Code != -1 {
		return response
	}

	body := request.Body

	queueIdInt := requestHeader.QueueId

	topicConfig := smp.BrokerController.TopicConfigManager.SelectTopicConfig(requestHeader.Topic)

	if queueIdInt < 0 {
		num := (smp.abstractSendMessageProcessor.Rand.Int31() % 99999999) % topicConfig.WriteQueueNums
		if num > 0 {
			queueIdInt = int32(num)
		} else {
			queueIdInt = -int32(num)
		}

	}

	sysFlag := requestHeader.SysFlag
	if stgcommon.MULTI_TAG == topicConfig.TopicFilterType {
		sysFlag |= sysflag.MultiTagsFlag
	}
	msgInner := new(stgstorelog.MessageExtBrokerInner)
	msgInner.Topic = requestHeader.Topic
	msgInner.Body = body
	msgInner.Flag = requestHeader.Flag
	message.SetPropertiesMap(&msgInner.Message, message.String2messageProperties(requestHeader.Properties))
	msgInner.PropertiesString = requestHeader.Properties
	msgInner.TagsCode = stgstorelog.TagsString2tagsCode(topicConfig.TopicFilterType, msgInner.GetTags())
	msgInner.QueueId = queueIdInt
	msgInner.SysFlag = sysFlag
	msgInner.BornTimestamp = requestHeader.BornTimestamp
	msgInner.BornHost = ctx.RemoteAddr().String()
	msgInner.StoreHost = smp.abstractSendMessageProcessor.StoreHost
	if requestHeader.ReconsumeTimes == 0 {
		msgInner.ReconsumeTimes = 0
	} else {
		msgInner.ReconsumeTimes = requestHeader.ReconsumeTimes
	}

	if smp.BrokerController.BrokerConfig.RejectTransactionMessage {
		traFlag := msgInner.GetProperty(message.PROPERTY_TRANSACTION_PREPARED)
		if len(traFlag) > 0 {
			response.Code = code.NO_PERMISSION
			response.Remark = "the broker[" + smp.BrokerController.BrokerConfig.BrokerIP1 + "] sending transaction message is forbidden"
			return response
		}
	}
	// TODO:当前messageStore有问题，只有SysFlag=8才能分发消息位置信息到ConsumeQueue
	msgInner.SysFlag = 8
	putMessageResult := smp.BrokerController.MessageStore.PutMessage(msgInner)
	if putMessageResult != nil {
		sendOK := false
		switch putMessageResult.PutMessageStatus {
		case stgstorelog.PUTMESSAGE_PUT_OK:
			sendOK = true
			response.Code = code.SUCCESS
		case stgstorelog.FLUSH_DISK_TIMEOUT:
			response.Code = code.FLUSH_DISK_TIMEOUT
			sendOK = true
		case stgstorelog.FLUSH_SLAVE_TIMEOUT:
			response.Code = code.FLUSH_SLAVE_TIMEOUT
			sendOK = true
		case stgstorelog.SLAVE_NOT_AVAILABLE:
			response.Code = code.SLAVE_NOT_AVAILABLE
			sendOK = true

		case stgstorelog.CREATE_MAPEDFILE_FAILED:
			response.Code = code.SYSTEM_ERROR
			response.Remark = "create maped file failed, please make sure OS and JDK both 64bit."
		case stgstorelog.MESSAGE_ILLEGAL:
			response.Code = code.MESSAGE_ILLEGAL
			response.Remark = "the message is illegal, maybe length not matched."
		case stgstorelog.SERVICE_NOT_AVAILABLE:
			response.Code = code.SERVICE_NOT_AVAILABLE
			response.Remark = "service not available now, maybe disk full, " + smp.diskUtil() + ", maybe your broker machine memory too small."
		case stgstorelog.PUTMESSAGE_UNKNOWN_ERROR:
			response.Code = code.SYSTEM_ERROR
			response.Remark = "UNKNOWN_ERROR"
		default:
			response.Code = code.SYSTEM_ERROR
			response.Remark = "UNKNOWN_ERROR DEFAULT"
		}

		if sendOK {
			smp.BrokerController.brokerStatsManager.IncTopicPutNums(msgInner.Topic)
			smp.BrokerController.brokerStatsManager.IncTopicPutSize(msgInner.Topic, putMessageResult.AppendMessageResult.WroteBytes)
			smp.BrokerController.brokerStatsManager.IncBrokerPutNums()

			response.Remark = ""
			responseHeader.MsgId = putMessageResult.AppendMessageResult.MsgId
			responseHeader.QueueId = queueIdInt
			responseHeader.QueueOffset = putMessageResult.AppendMessageResult.LogicsOffset

			DoResponse(ctx, request, response)
			if smp.BrokerController.BrokerConfig.LongPollingEnable {
				smp.BrokerController.PullRequestHoldService.notifyMessageArriving(
					requestHeader.Topic, queueIdInt, putMessageResult.AppendMessageResult.LogicsOffset+1)
			}

			// 消息轨迹：记录发送成功的消息
			if smp.HasSendMessageHook() {
				mqtraceContext.MsgId = responseHeader.MsgId
				mqtraceContext.QueueId = responseHeader.QueueId
				mqtraceContext.QueueOffset = responseHeader.QueueOffset
			}
			logger.Infof("response:%#v", response)
			return nil
		}

	} else {
		response.Code = code.SYSTEM_ERROR
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
func (smp *SendMessageProcessor) RegisterSendMessageHook(sendMessageHookList []mqtrace.SendMessageHook) {
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
func (smp *SendMessageProcessor) RegisterConsumeMessageHook(consumeMessageHookList []mqtrace.ConsumeMessageHook) {
	smp.consumeMessageHookList = consumeMessageHookList
}

// ExecuteConsumeMessageHookAfter 消费消息后执行回调
// Author rongzhihong
// Since 2017/9/5
func (smp *SendMessageProcessor) ExecuteConsumeMessageHookAfter(context *mqtrace.ConsumeMessageContext) {
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
	storePathPhysic := smp.BrokerController.MessageStoreConfig.StorePathCommitLog

	physicRatio := stgcommon.GetDiskPartitionSpaceUsedPercent(storePathPhysic)

	storePathLogis := config.GetStorePathConsumeQueue(smp.BrokerController.MessageStoreConfig.StorePathRootDir)

	logisRatio := stgcommon.GetDiskPartitionSpaceUsedPercent(storePathLogis)

	storePathIndex := config.GetStorePathConsumeQueue(smp.BrokerController.MessageStoreConfig.StorePathRootDir)

	indexRatio := stgcommon.GetDiskPartitionSpaceUsedPercent(storePathIndex)

	return fmt.Sprintf("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio)
}

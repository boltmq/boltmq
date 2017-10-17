package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/longpolling"
	"git.oschina.net/cloudzone/smartgo/stgbroker/mqtrace"
	"git.oschina.net/cloudzone/smartgo/stgbroker/pagecache"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/filter"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/topic"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
	"strconv"
)

// PullMessageProcessor 拉消息请求处理
// Author gaoyanlei
// Since 2017/8/10
type PullMessageProcessor struct {
	BrokerController       *BrokerController
	ConsumeMessageHookList []mqtrace.ConsumeMessageHook
}

// NewPullMessageProcessor 初始化PullMessageProcessor
// Author gaoyanlei
// Since 2017/8/9
func NewPullMessageProcessor(brokerController *BrokerController) *PullMessageProcessor {
	var pullMessageProcessor = new(PullMessageProcessor)
	pullMessageProcessor.BrokerController = brokerController
	return pullMessageProcessor
}

func (pull *PullMessageProcessor) ProcessRequest(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {

	return pull.processRequest(request, ctx, true)
}

// ExecuteRequestWhenWakeup  唤醒拉取消息的请求
// Author rongzhihong
// Since 2017/9/5
func (pull *PullMessageProcessor) ExecuteRequestWhenWakeup(ctx netm.Context, request *protocol.RemotingCommand) {
	go func() {
		logger.Info("唤醒HoldPullRequest: ExtFields:%v, Opaque:%d", request.ExtFields, request.Opaque)

		response, err := pull.processRequest(request, ctx, false)
		if err != nil {
			logger.Errorf("ExecuteRequestWhenWakeup run, throw error:%s", err.Error())
			return
		}

		if response == nil {
			return
		}

		response.Opaque = request.Opaque
		response.MarkResponseType()

		_, err = ctx.WriteSerialObject(response)
		if err != nil {
			logger.Errorf("processRequestWrapper response to %s failed %s. \n request:%s, response:%s",
				ctx.RemoteAddr().String(), err.Error(), request.ToString(), response.ToString())
		}
		logger.Infof("............唤醒HoldPullRequest response:%#v", response)
	}()
}

func (pull *PullMessageProcessor) processRequest(request *protocol.RemotingCommand, ctx netm.Context, brokerAllowSuspend bool) (*protocol.RemotingCommand, error) {
	responseHeader := &header.PullMessageResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	requestHeader := &header.PullMessageRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	response.Opaque = request.Opaque

	// 检查Broker权限
	if !pull.BrokerController.BrokerConfig.HasReadable() {
		response.Code = code.NO_PERMISSION
		response.Remark = "the broker[" + pull.BrokerController.BrokerConfig.BrokerIP1 + "] pulling message is forbidden"
		return response, nil
	}

	// 确保订阅组存在
	subscriptionGroupConfig := pull.BrokerController.SubscriptionGroupManager.FindSubscriptionGroupConfig(
		requestHeader.ConsumerGroup)
	if nil == subscriptionGroupConfig {
		response.Code = code.SUBSCRIPTION_GROUP_NOT_EXIST
		response.Remark = "subscription group not exist, " + requestHeader.ConsumerGroup
		return response, nil
	}

	// 这个订阅组是否可以消费消息
	if !subscriptionGroupConfig.ConsumeEnable {
		response.Code = code.NO_PERMISSION
		response.Remark = "subscription group no permission, " + requestHeader.ConsumerGroup
		return response, nil
	}

	hasSuspendFlag := sysflag.HasSuspendFlag(requestHeader.SysFlag)
	hasCommitOffsetFlag := sysflag.HasCommitOffsetFlag(requestHeader.SysFlag)
	hasSubscriptionFlag := sysflag.HasSubscriptionFlag(requestHeader.SysFlag)

	suspendTimeoutMillisLong := requestHeader.SuspendTimeoutMillis
	if hasSuspendFlag == false {
		suspendTimeoutMillisLong = 0
	}

	// 检查topic是否存在
	topicConfig := pull.BrokerController.TopicConfigManager.SelectTopicConfig(requestHeader.Topic)
	if nil == topicConfig {
		response.Code = code.TOPIC_NOT_EXIST
		response.Remark = "topic[" + requestHeader.Topic + "] not exist, apply first please!"
		return response, nil
	}

	// 检查topic权限
	if !constant.IsReadable(topicConfig.Perm) {
		response.Code = code.NO_PERMISSION
		response.Remark = "the topic[" + requestHeader.Topic + "] pulling message is forbidden"
		return response, nil
	}

	// 检查队列有效性
	if requestHeader.QueueId < 0 || requestHeader.QueueId >= topicConfig.ReadQueueNums {
		errorInfo := "queueId[" + strconv.Itoa(int(requestHeader.QueueId)) + "] is illagal,Topic :" + requestHeader.Topic + " topicConfig.readQueueNums: " + strconv.Itoa(int(topicConfig.ReadQueueNums))
		logger.Warn(errorInfo)
		response.Code = code.SYSTEM_ERROR
		response.Remark = errorInfo
		return response, nil
	}
	// 订阅关系处理
	subscriptionData := &heartbeat.SubscriptionData{}
	if hasSubscriptionFlag {
		var err error
		subscriptionData, err = filter.BuildSubscriptionData4Ponit(requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.Subscription)
		if err != nil {
			logger.Warnf("parse the consumer's subscription %s failed, group: %s", requestHeader.Subscription, requestHeader.ConsumerGroup)
			response.Code = code.SUBSCRIPTION_PARSE_FAILED
			response.Remark = "parse the consumer's subscription failed"
			return response, nil
		}
	} else {
		// 如果没有获取到维护的consumerGroup信息，则返回
		consumerGroupInfo := pull.BrokerController.ConsumerManager.GetConsumerGroupInfo(requestHeader.ConsumerGroup)
		if nil == consumerGroupInfo {
			logger.Warnf("the consumer's group info not exist, group: %s", requestHeader.ConsumerGroup)
			response.Code = code.SUBSCRIPTION_NOT_EXIST
			response.Remark = "the consumer's group info not exist"
			return response, nil
		}

		if !subscriptionGroupConfig.ConsumeBroadcastEnable && consumerGroupInfo.MessageModel == heartbeat.BROADCASTING {
			response.Code = code.NO_PERMISSION
			response.Remark = "the consumer group[" + requestHeader.ConsumerGroup
			return response, nil
		}

		subscriptionData = consumerGroupInfo.FindSubscriptionData(requestHeader.Topic)
		if nil == subscriptionData {
			logger.Warnf("the consumer's subscription not exist, group: %s", requestHeader.ConsumerGroup)
			response.Code = code.SUBSCRIPTION_NOT_EXIST
			response.Remark = "the consumer's subscription not exist"
			return response, nil
		}

		// 判断Broker的订阅关系版本是否最新
		if subscriptionData.SubVersion < requestHeader.SubVersion {
			logger.Warnf("the broker's subscription is not latest, group: %s %s", requestHeader.ConsumerGroup, subscriptionData.SubString)
			response.Code = code.SUBSCRIPTION_NOT_LATEST
			response.Remark = "the consumer's subscription not latestGetMessageResult"
			return response, nil
		}
	}

	getMessageResult := pull.BrokerController.MessageStore.GetMessage(requestHeader.ConsumerGroup, requestHeader.Topic,
		requestHeader.QueueId, requestHeader.QueueOffset, int32(requestHeader.MaxMsgNums), subscriptionData)
	if nil != getMessageResult {
		response.Remark = getMessageResult.Status.String()
		responseHeader.NextBeginOffset = getMessageResult.NextBeginOffset
		responseHeader.MinOffset = getMessageResult.MinOffset
		responseHeader.MaxOffset = getMessageResult.MaxOffset

		// 消费较慢，重定向到另外一台机器
		if getMessageResult.SuggestPullingFromSlave {
			responseHeader.SuggestWhichBrokerId = subscriptionGroupConfig.WhichBrokerWhenConsumeSlowly
		} else {
			// 消费正常，按照订阅组配置重定向
			responseHeader.SuggestWhichBrokerId = subscriptionGroupConfig.BrokerId
		}

		switch getMessageResult.Status {
		case stgstorelog.FOUND:
			response.Code = code.SUCCESS

			// 消息轨迹：记录客户端拉取的消息记录（不表示消费成功）
			if pull.hasConsumeMessageHook() {
				// 执行hook
				context := new(mqtrace.ConsumeMessageContext)
				context.ConsumerGroup = requestHeader.ConsumerGroup
				context.Topic = requestHeader.Topic
				context.ClientHost = ctx.LocalAddr().String()
				context.StoreHost = pull.BrokerController.GetBrokerAddr()
				context.QueueId = requestHeader.QueueId

				storeHost := pull.BrokerController.GetStoreHost()
				messageIds := pull.BrokerController.MessageStore.GetMessageIds(requestHeader.Topic, requestHeader.QueueId, requestHeader.QueueOffset, requestHeader.QueueOffset+int64(getMessageResult.GetMessageCount()), storeHost)
				context.MessageIds = messageIds
				context.BodyLength = getMessageResult.BufferTotalSize / getMessageResult.GetMessageCount()
				pull.ExecuteConsumeMessageHookBefore(context)
			}
		case stgstorelog.MESSAGE_WAS_REMOVING:
			response.Code = code.PULL_RETRY_IMMEDIATELY
			// 这两个返回值都表示服务器暂时没有这个队列，应该立刻将客户端Offset重置为0
		case stgstorelog.NO_MATCHED_LOGIC_QUEUE:
			fallthrough
		case stgstorelog.NO_MESSAGE_IN_QUEUE:
			if 0 != requestHeader.QueueOffset {
				response.Code = code.PULL_OFFSET_MOVED
				// XXX: warn and notify me
				logger.Warnf("the broker store no queue data, "+
					"fix the request offset %d to %d, Topic: %s QueueId: %d Consumer Group: %s",
					requestHeader.QueueOffset,
					getMessageResult.NextBeginOffset,
					requestHeader.Topic,
					requestHeader.QueueId,
					requestHeader.ConsumerGroup,
				)
			} else {
				response.Code = code.PULL_NOT_FOUND
			}
		case stgstorelog.NO_MATCHED_MESSAGE:
			response.Code = code.PULL_RETRY_IMMEDIATELY
		case stgstorelog.OFFSET_FOUND_NULL:
			response.Code = code.PULL_NOT_FOUND
		case stgstorelog.OFFSET_OVERFLOW_BADLY:
			response.Code = code.PULL_OFFSET_MOVED
			logger.Infof("the request offset: %d over flow badly, broker max offset: %d, consumer: %s", requestHeader.QueueOffset, getMessageResult.MaxOffset, ctx.LocalAddr().String())
		case stgstorelog.OFFSET_OVERFLOW_ONE:
			response.Code = code.PULL_NOT_FOUND
		case stgstorelog.OFFSET_TOO_SMALL:
			response.Code = code.PULL_OFFSET_MOVED
			logger.Infof("the request offset: %d too small, broker min offset: %d, consumer: %s", requestHeader.QueueOffset, getMessageResult.MinOffset, ctx.LocalAddr().String())
		default:
		}

		switch response.Code {
		case code.SUCCESS:
			// 统计
			pull.BrokerController.brokerStatsManager.IncGroupGetNums(requestHeader.ConsumerGroup, requestHeader.Topic, getMessageResult.GetMessageCount())
			pull.BrokerController.brokerStatsManager.IncGroupGetSize(requestHeader.ConsumerGroup, requestHeader.Topic, getMessageResult.BufferTotalSize)
			pull.BrokerController.brokerStatsManager.IncBrokerGetNums(getMessageResult.GetMessageCount())

			manyMessageTransfer := pagecache.NewManyMessageTransfer(response, getMessageResult)
			_, err = ctx.WriteSerialObject(manyMessageTransfer)
			if err != nil {
				logger.Errorf("transfer many message by pagecache failed, RemoteAddr:%s, Error:%s",
					ctx.RemoteAddr().String(), err.Error())
			}
			// TODO getMessageResult.Release()
			response = nil
		case code.PULL_NOT_FOUND:
			// 长轮询
			if brokerAllowSuspend && hasSuspendFlag {
				logger.Infof("进入hold pull: ExtFields=%#v, Opaque=%d", request.ExtFields, request.Opaque)
				pollingTimeMills := suspendTimeoutMillisLong
				if !pull.BrokerController.BrokerConfig.LongPollingEnable {
					pollingTimeMills = pull.BrokerController.BrokerConfig.ShortPollingTimeMills
				}

				suspendTimestamp := pull.BrokerController.MessageStore.Now()
				pullRequest := longpolling.NewPullRequest(request, ctx, int64(pollingTimeMills), suspendTimestamp, requestHeader.QueueOffset)
				pull.BrokerController.PullRequestHoldService.SuspendPullRequest(requestHeader.Topic, requestHeader.QueueId, pullRequest)
				response = nil
			}
		case code.PULL_RETRY_IMMEDIATELY:
		case code.PULL_OFFSET_MOVED:
			if pull.BrokerController.MessageStoreConfig.BrokerRole != config.SLAVE ||
				pull.BrokerController.BrokerConfig.OffsetCheckInSlave {

				mq := message.MessageQueue{
					Topic:      requestHeader.Topic,
					QueueId:    int(requestHeader.QueueId),
					BrokerName: pull.BrokerController.BrokerConfig.BrokerName,
				}

				event := &topic.OffsetMovedEvent{
					ConsumerGroup: requestHeader.ConsumerGroup,
					MessageQueue:  mq,
					OffsetRequest: requestHeader.QueueOffset,
					OffsetNew:     getMessageResult.NextBeginOffset,
				}

				pull.generateOffsetMovedEvent(event)
			} else {
				responseHeader.SuggestWhichBrokerId = subscriptionGroupConfig.BrokerId
				response.Code = code.PULL_RETRY_IMMEDIATELY
			}

			logger.Warnf("PULL_OFFSET_MOVED:topic=%s, groupId=%d, clientId=%d, offset=%d, suggestBrokerId=%d",
				requestHeader.Topic, requestHeader.ConsumerGroup, requestHeader.QueueOffset, responseHeader.SuggestWhichBrokerId)
		default:
		}
	} else {
		response.Code = code.SYSTEM_ERROR
		response.Remark = "store getMessage return null"
	}

	// 存储Consumer消费进度
	storeOffsetEnable := brokerAllowSuspend                      // 说明是首次调用，相对于长轮询通知
	storeOffsetEnable = storeOffsetEnable && hasCommitOffsetFlag // 说明Consumer设置了标志位
	// 只有Master支持存储offset
	storeOffsetEnable = storeOffsetEnable && pull.BrokerController.MessageStoreConfig.BrokerRole != config.SLAVE

	//logger.Infof("brokerAllowSuspend:%v, requestHeader.SysFlag:%v,  hasCommitOffsetFlag:%v, storeOffsetEnable:%v, requestHeader:%#v",
	//	brokerAllowSuspend, requestHeader.SysFlag, hasCommitOffsetFlag, storeOffsetEnable, requestHeader)

	if storeOffsetEnable {
		pull.BrokerController.ConsumerOffsetManager.CommitOffset(requestHeader.ConsumerGroup,
			requestHeader.Topic, int(requestHeader.QueueId), requestHeader.CommitOffset)
	}

	return response, nil
}

func (pull *PullMessageProcessor) hasConsumeMessageHook() bool {
	return pull.ConsumeMessageHookList != nil && len(pull.ConsumeMessageHookList) > 0
}

// generateOffsetMovedEvent 偏移量移动事件
// Author rongzhihong
// Since 2017/9/17
func (pull *PullMessageProcessor) generateOffsetMovedEvent(event *topic.OffsetMovedEvent) {
	defer utils.RecoveredFn()

	msgInner := new(stgstorelog.MessageExtBrokerInner)
	msgInner.Topic = stgcommon.OFFSET_MOVED_EVENT
	msgInner.SetTags(event.ConsumerGroup)
	msgInner.SetDelayTimeLevel(0)
	msgInner.SetKeys(event.ConsumerGroup)
	msgInner.Body = event.CustomEncode(event)
	msgInner.Flag = 0
	msgInner.TagsCode = stgstorelog.TagsString2tagsCode(stgcommon.SINGLE_TAG, msgInner.GetTags())

	msgInner.QueueId = int32(0)
	msgInner.SysFlag = 0
	msgInner.BornTimestamp = timeutil.CurrentTimeMillis()
	msgInner.BornHost = pull.BrokerController.GetBrokerAddr()
	msgInner.StoreHost = msgInner.BornHost

	msgInner.ReconsumeTimes = 0

	pull.BrokerController.MessageStore.PutMessage(msgInner)
}

// ConsumeMessageHook 消费消息回调
// Author rongzhihong
// Since 2017/9/11
func (pull *PullMessageProcessor) RegisterConsumeMessageHook(consumeMessageHookList []mqtrace.ConsumeMessageHook) {
	pull.ConsumeMessageHookList = consumeMessageHookList
}

// ExecuteConsumeMessageHookBefore 消费消息前，执行回调
// Author rongzhihong
// Since 2017/9/11
func (pull *PullMessageProcessor) ExecuteConsumeMessageHookBefore(context *mqtrace.ConsumeMessageContext) {
	defer utils.RecoveredFn()

	if pull.hasConsumeMessageHook() {
		for _, hook := range pull.ConsumeMessageHookList {
			hook.ConsumeMessageBefore(context)
		}
	}
}

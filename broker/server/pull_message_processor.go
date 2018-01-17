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

	"github.com/boltmq/boltmq/broker/server/longpolling"
	"github.com/boltmq/boltmq/broker/server/pagecache"
	"github.com/boltmq/boltmq/broker/trace"
	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/boltmq/net/core"
	"github.com/boltmq/boltmq/store"
	"github.com/boltmq/boltmq/store/persistent"
	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/constant"
	"github.com/boltmq/common/filter"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/head"
	"github.com/boltmq/common/protocol/heartbeat"
	"github.com/boltmq/common/sysflag"
	"github.com/boltmq/common/utils/system"
)

// pullMessageProcessor 拉消息请求处理
// Author gaoyanlei
// Since 2017/8/10
type pullMessageProcessor struct {
	brokerController *BrokerController
	csmMsgHookList   []trace.ConsumeMessageHook
}

// newPullMessageProcessor 初始化pullMessageProcessor
// Author gaoyanlei
// Since 2017/8/9
func newPullMessageProcessor(brokerController *BrokerController) *pullMessageProcessor {
	var pmsgp = new(pullMessageProcessor)
	pmsgp.brokerController = brokerController
	return pmsgp
}

func (pmsgp *pullMessageProcessor) ProcessRequest(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	return pmsgp.processRequest(request, ctx, true)
}

// executeRequestWhenWakeup  唤醒拉取消息的请求
// Author rongzhihong
// Since 2017/9/5
func (pmsgp *pullMessageProcessor) executeRequestWhenWakeup(ctx core.Context, request *protocol.RemotingCommand) {
	go func() {
		//logger.Info("....唤醒HoldPullRequest: ExtFields:%v, Opaque:%d", request.ExtFields, request.Opaque)
		response, err := pmsgp.processRequest(request, ctx, false)
		if err != nil {
			logger.Errorf("executeRequestWhenWakeup run, throw error: %s.", err)
			return
		}
		if response == nil {
			return
		}

		if ctx.Closed() {
			return
		}

		response.Opaque = request.Opaque
		response.MarkResponseType()
		_, err = ctx.WriteSerialData(response)
		if err != nil {
			logger.Errorf("pullMessageHold response to %s failed. error:%s. ### request:%s, ### response:%s.",
				ctx.RemoteAddr(), err, request, response)
		}
	}()
}

func (pmsgp *pullMessageProcessor) processRequest(request *protocol.RemotingCommand, ctx core.Context, brokerAllowSuspend bool) (*protocol.RemotingCommand, error) {
	responseHeader := &head.PullMessageResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	requestHeader := &head.PullMessageRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("pull message: decode request throw error: %s.", err)
	}

	response.Opaque = request.Opaque

	// 检查Broker权限
	if !pmsgp.brokerController.cfg.HasReadable() {
		response.Code = protocol.NO_PERMISSION
		response.Remark = "the broker[" + pmsgp.brokerController.cfg.Broker.IP + "] pulling message is forbidden"
		return response, nil
	}

	// 确保订阅组存在
	subscriptionGroupConfig := pmsgp.brokerController.subGroupManager.findSubscriptionGroupConfig(
		requestHeader.ConsumerGroup)
	if nil == subscriptionGroupConfig {
		response.Code = protocol.SUBSCRIPTION_GROUP_NOT_EXIST
		response.Remark = "subscription group not exist, " + requestHeader.ConsumerGroup
		return response, nil
	}

	// 这个订阅组是否可以消费消息
	if !subscriptionGroupConfig.ConsumeEnable {
		response.Code = protocol.NO_PERMISSION
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
	topicConfig := pmsgp.brokerController.tpConfigManager.selectTopicConfig(requestHeader.Topic)
	if nil == topicConfig {
		response.Code = protocol.TOPIC_NOT_EXIST
		response.Remark = "topic[" + requestHeader.Topic + "] not exist, apply first please!"
		return response, nil
	}

	// 检查topic权限
	if !constant.IsReadable(topicConfig.Perm) {
		response.Code = protocol.NO_PERMISSION
		response.Remark = "the topic[" + requestHeader.Topic + "] pulling message is forbidden"
		return response, nil
	}

	// 检查队列有效性
	if requestHeader.QueueId < 0 || requestHeader.QueueId >= topicConfig.ReadQueueNums {
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = fmt.Sprintf("queueId[%d] is illagal, topic: %s, read queue nums: %d.",
			requestHeader.QueueId, requestHeader.Topic, topicConfig.ReadQueueNums)
		logger.Warnf(response.Remark)

		return response, nil
	}

	// 订阅关系处理
	subscriptionData := &heartbeat.SubscriptionData{}
	if hasSubscriptionFlag {
		var err error
		subscriptionData, err = filter.BuildSubscriptionData4Ponit(requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.Subscription)
		if err != nil {
			logger.Warnf("parse the consumer's subscription %s failed, group: %s.",
				requestHeader.Subscription, requestHeader.ConsumerGroup)
			response.Code = protocol.SUBSCRIPTION_PARSE_FAILED
			response.Remark = "parse the consumer's subscription failed"
			return response, nil
		}
	} else {
		// 如果没有获取到维护的consumerGroup信息，则返回
		consumerGroupInfo := pmsgp.brokerController.csmManager.getConsumerGroupInfo(requestHeader.ConsumerGroup)
		if nil == consumerGroupInfo {
			logger.Warnf("the consumer's group info not exist, group: %s.", requestHeader.ConsumerGroup)
			response.Code = protocol.SUBSCRIPTION_NOT_EXIST
			response.Remark = "the consumer's group info not exist"
			return response, nil
		}

		if !subscriptionGroupConfig.ConsumeBroadcastEnable && consumerGroupInfo.msgModel == heartbeat.BROADCASTING {
			response.Code = protocol.NO_PERMISSION
			response.Remark = fmt.Sprintf("the consumer group[%s] can not consume by broadcast way", requestHeader.ConsumerGroup)
			return response, nil
		}

		subscriptionData = consumerGroupInfo.findSubscriptionData(requestHeader.Topic)
		if nil == subscriptionData {
			logger.Warnf("the consumer's subscription not exist, group: %s.", requestHeader.ConsumerGroup)
			response.Code = protocol.SUBSCRIPTION_NOT_EXIST
			response.Remark = "the consumer's subscription not exist"
			return response, nil
		}

		// 判断Broker的订阅关系版本是否最新
		if subscriptionData.SubVersion < requestHeader.SubVersion {
			logger.Warnf("the broker's subscription is not latest, group: %s %s.",
				requestHeader.ConsumerGroup, subscriptionData.SubString)
			response.Code = protocol.SUBSCRIPTION_NOT_LATEST
			response.Remark = "the consumer's subscription not latestGetMessageResult"
			return response, nil
		}
	}

	getMessageResult := pmsgp.brokerController.messageStore.GetMessage(requestHeader.ConsumerGroup, requestHeader.Topic,
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
		case store.FOUND:
			response.Code = protocol.SUCCESS

			// 消息轨迹：记录客户端拉取的消息记录（不表示消费成功）
			if pmsgp.hasConsumeMessageHook() {
				// 执行hook
				context := new(trace.ConsumeMessageContext)
				context.ConsumerGroup = requestHeader.ConsumerGroup
				context.Topic = requestHeader.Topic
				context.ClientHost = ctx.RemoteAddr().String()
				context.StoreHost = pmsgp.brokerController.getBrokerAddr()
				context.QueueId = requestHeader.QueueId

				storeHost := pmsgp.brokerController.getStoreHost()
				messageIds := pmsgp.brokerController.messageStore.MessageIds(requestHeader.Topic, requestHeader.QueueId, requestHeader.QueueOffset, requestHeader.QueueOffset+int64(getMessageResult.GetMessageCount()), storeHost)
				context.MessageIds = messageIds
				context.BodyLength = getMessageResult.BufferTotalSize / getMessageResult.GetMessageCount()
				pmsgp.executeConsumeMessageHookBefore(context)
			}
		case store.MESSAGE_WAS_REMOVING:
			response.Code = protocol.PULL_RETRY_IMMEDIATELY
			// 这两个返回值都表示服务器暂时没有这个队列，应该立刻将客户端Offset重置为0
		case store.NO_MATCHED_LOGIC_QUEUE:
			fallthrough
		case store.NO_MESSAGE_IN_QUEUE:
			if 0 != requestHeader.QueueOffset {
				response.Code = protocol.PULL_OFFSET_MOVED
				// XXX: warn and notify me
				logger.Warnf("the broker store no queue data, fix the request offset %d to %d, Topic: %s QueueId: %d Consumer Group: %s.",
					requestHeader.QueueOffset,
					getMessageResult.NextBeginOffset,
					requestHeader.Topic,
					requestHeader.QueueId,
					requestHeader.ConsumerGroup,
				)
			} else {
				response.Code = protocol.PULL_NOT_FOUND
			}
		case store.NO_MATCHED_MESSAGE:
			response.Code = protocol.PULL_RETRY_IMMEDIATELY
		case store.OFFSET_FOUND_NULL:
			response.Code = protocol.PULL_NOT_FOUND
		case store.OFFSET_OVERFLOW_BADLY:
			response.Code = protocol.PULL_OFFSET_MOVED
			logger.Infof("the request offset: %d over flow badly, broker max offset: %d, consumer: %s.",
				requestHeader.QueueOffset, getMessageResult.MaxOffset, ctx.LocalAddr().String())
		case store.OFFSET_OVERFLOW_ONE:
			response.Code = protocol.PULL_NOT_FOUND
		case store.OFFSET_TOO_SMALL:
			response.Code = protocol.PULL_OFFSET_MOVED
			logger.Infof("the request offset: %d too small, broker min offset: %d, consumer: %s.",
				requestHeader.QueueOffset, getMessageResult.MinOffset, ctx.LocalAddr().String())
		default:
		}

		switch response.Code {
		case protocol.SUCCESS:
			// 统计
			pmsgp.brokerController.brokerStats.IncGroupGetNums(requestHeader.ConsumerGroup, requestHeader.Topic, getMessageResult.GetMessageCount())
			pmsgp.brokerController.brokerStats.IncGroupGetSize(requestHeader.ConsumerGroup, requestHeader.Topic, getMessageResult.BufferTotalSize)
			pmsgp.brokerController.brokerStats.IncBrokerGetNums(getMessageResult.GetMessageCount())

			manyMessageTransfer := pagecache.NewManyMessageTransfer(response, getMessageResult)
			_, err = ctx.WriteSerialData(manyMessageTransfer)
			if err != nil {
				logger.Errorf("transfer many message by pagecache failed, RemoteAddr:%s, Error:%s.",
					ctx.RemoteAddr().String(), err.Error())
			}
			getMessageResult.Release()
			response = nil
		case protocol.PULL_NOT_FOUND:
			// 长轮询
			if brokerAllowSuspend && hasSuspendFlag {
				//logger.Infof("进入hold pmsgp: ExtFields=%#v, Opaque=%d", request.ExtFields, request.Opaque)
				pollingTimeMills := suspendTimeoutMillisLong
				if !pmsgp.brokerController.cfg.Broker.LongPollingEnable {
					pollingTimeMills = pmsgp.brokerController.cfg.Broker.ShortPollingTimeMills
				}

				suspendTimestamp := system.CurrentTimeMillis()
				pullRequest := longpolling.NewPullRequest(request, ctx, int64(pollingTimeMills), suspendTimestamp, requestHeader.QueueOffset)
				pmsgp.brokerController.pullRequestHoldSrv.suspendPullRequest(requestHeader.Topic, requestHeader.QueueId, pullRequest)
				response = nil
			}
		case protocol.PULL_RETRY_IMMEDIATELY:
		case protocol.PULL_OFFSET_MOVED:
			if pmsgp.brokerController.storeCfg.BrokerRole != persistent.SLAVE ||
				pmsgp.brokerController.cfg.Broker.OffsetCheckInSlave {

				mq := message.MessageQueue{
					Topic:      requestHeader.Topic,
					QueueId:    int(requestHeader.QueueId),
					BrokerName: pmsgp.brokerController.cfg.Cluster.BrokerName,
				}

				event := &protocol.OffsetMovedEvent{
					ConsumerGroup: requestHeader.ConsumerGroup,
					MessageQueue:  mq,
					OffsetRequest: requestHeader.QueueOffset,
					OffsetNew:     getMessageResult.NextBeginOffset,
				}

				pmsgp.generateOffsetMovedEvent(event)
			} else {
				responseHeader.SuggestWhichBrokerId = subscriptionGroupConfig.BrokerId
				response.Code = protocol.PULL_RETRY_IMMEDIATELY
			}

			logger.Warnf("PULL_OFFSET_MOVED:topic=%s, groupId=%d, clientId=%d, offset=%d, suggestBrokerId=%d.",
				requestHeader.Topic, requestHeader.ConsumerGroup, requestHeader.QueueOffset, responseHeader.SuggestWhichBrokerId)
		default:
		}
	} else {
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = "store getMessage return null"
	}

	// 存储Consumer消费进度
	storeOffsetEnable := brokerAllowSuspend                      // 说明是首次调用，相对于长轮询通知
	storeOffsetEnable = storeOffsetEnable && hasCommitOffsetFlag // 说明Consumer设置了标志位
	// 只有Master支持存储offset
	storeOffsetEnable = storeOffsetEnable && pmsgp.brokerController.storeCfg.BrokerRole != persistent.SLAVE

	if storeOffsetEnable {
		pmsgp.brokerController.csmOffsetManager.commitOffset(requestHeader.ConsumerGroup,
			requestHeader.Topic, int(requestHeader.QueueId), requestHeader.CommitOffset)
	}

	return response, nil
}

func (pmsgp *pullMessageProcessor) hasConsumeMessageHook() bool {
	return pmsgp.csmMsgHookList != nil && len(pmsgp.csmMsgHookList) > 0
}

// generateOffsetMovedEvent 偏移量移动事件
// Author rongzhihong
// Since 2017/9/17
func (pmsgp *pullMessageProcessor) generateOffsetMovedEvent(event *protocol.OffsetMovedEvent) {
	msgInner := new(store.MessageExtInner)
	msgInner.Topic = basis.OFFSET_MOVED_EVENT
	msgInner.SetTags(event.ConsumerGroup)
	msgInner.SetDelayTimeLevel(0)
	msgInner.SetKeys(event.ConsumerGroup)
	msgInner.Flag = 0
	msgInner.PropertiesString = message.MessageProperties2String(msgInner.Properties)
	msgInner.TagsCode = basis.TagsString2tagsCode(basis.SINGLE_TAG, msgInner.GetTags())

	msgInner.Body, _ = common.Encode(event)
	msgInner.QueueId = int32(0)
	msgInner.SysFlag = 0
	msgInner.BornTimestamp = system.CurrentTimeMillis()
	msgInner.BornHost = pmsgp.brokerController.getBrokerAddr()

	msgInner.StoreHost = msgInner.BornHost

	msgInner.ReconsumeTimes = 0

	pmsgp.brokerController.messageStore.PutMessage(msgInner)
}

// registerConsumeMessageHook 消费消息回调
// Author rongzhihong
// Since 2017/9/11
func (pmsgp *pullMessageProcessor) RegisterConsumeMessageHook(consumeMessageHookList []trace.ConsumeMessageHook) {
	pmsgp.csmMsgHookList = consumeMessageHookList
}

// executeConsumeMessageHookBefore 消费消息前，执行回调
// Author rongzhihong
// Since 2017/9/11
func (pmsgp *pullMessageProcessor) executeConsumeMessageHookBefore(context *trace.ConsumeMessageContext) {
	if pmsgp.hasConsumeMessageHook() {
		for _, hook := range pmsgp.csmMsgHookList {
			hook.ConsumeMessageBefore(context)
		}
	}
}

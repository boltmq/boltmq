package stgbroker

import (
	"container/list"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker/mqtrace"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/filter"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	commonprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/topic"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
	"strconv"
)

// PullMessageProcessor 拉消息请求处理
// Author gaoyanlei
// Since 2017/8/10
type PullMessageProcessor struct {
	// TODO Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

	BrokerController       *BrokerController
	ConsumeMessageHookList list.List
}

// NewPullMessageProcessor 初始化PullMessageProcessor
// Author gaoyanlei
// Since 2017/8/9
func NewPullMessageProcessor(brokerController *BrokerController) *PullMessageProcessor {
	var pullMessageProcessor = new(PullMessageProcessor)
	pullMessageProcessor.BrokerController = brokerController
	return pullMessageProcessor
}

func (self *PullMessageProcessor) ProcessRequest(request protocol.RemotingCommand, // TODO ChannelHandlerContext ctx
) *protocol.RemotingCommand {

	return self.processRequest(request, true)
}

func (self *PullMessageProcessor) processRequest(request protocol.RemotingCommand, // TODO ChannelHandlerContext ctx
	brokerAllowSuspend bool) *protocol.RemotingCommand {
	response := &protocol.RemotingCommand{}
	responseHeader := &header.PullMessageResponseHeader{}
	requestHeader := &header.PullMessageRequestHeader{}
	response.Opaque = (request.Opaque)

	// TODO if (log.isDebugEnabled()) {
	// TODO	log.debug("receive PullMessage request command, " + request);
	// TODO }

	// 检查Broker权限
	if !constant.IsReadable(self.BrokerController.BrokerConfig.BrokerPermission) {
		response.Code = commonprotocol.NO_PERMISSION
		response.Remark = "the broker[" + self.BrokerController.BrokerConfig.BrokerIP1 + "] pulling message is forbidden"
		return response
	}

	// 确保订阅组存在
	subscriptionGroupConfig := self.BrokerController.SubscriptionGroupManager.findSubscriptionGroupConfig(
		requestHeader.ConsumerGroup)
	if nil == subscriptionGroupConfig {
		response.Code = commonprotocol.SUBSCRIPTION_GROUP_NOT_EXIST
		response.Remark = "subscription group not exist, " + requestHeader.ConsumerGroup
		return response
	}

	// 这个订阅组是否可以消费消息
	if !subscriptionGroupConfig.ConsumeEnable {
		response.Code = commonprotocol.NO_PERMISSION
		response.Remark = "subscription group no permission, " + requestHeader.ConsumerGroup
		return response
	}

	hasSuspendFlag := sysflag.HasSuspendFlag(requestHeader.SysFlag)
	hasCommitOffsetFlag := sysflag.HasCommitOffsetFlag(requestHeader.SysFlag)
	hasSubscriptionFlag := sysflag.HasSubscriptionFlag(requestHeader.SysFlag)

	suspendTimeoutMillisLong := requestHeader.SuspendTimeoutMillis
	if hasSuspendFlag {
		suspendTimeoutMillisLong = 0
	}

	// 检查topic是否存在
	topicConfig := self.BrokerController.TopicConfigManager.selectTopicConfig(requestHeader.Topic)
	if nil == topicConfig {
		response.Code = commonprotocol.TOPIC_NOT_EXIST
		response.Remark = "topic[" + requestHeader.Topic + "] not exist, apply first please!"
		return response
	}

	// 检查topic权限
	if !constant.IsReadable(topicConfig.Perm) {
		response.Code = commonprotocol.NO_PERMISSION
		response.Remark = "the topic[" + requestHeader.Topic + "] pulling message is forbidden"
		return response
	}

	// 检查队列有效性
	if requestHeader.QueueId < 0 || requestHeader.QueueId >= topicConfig.ReadQueueNums {
		errorInfo := "queueId[" + strconv.Itoa(requestHeader.QueueId) + "] is illagal,Topic :" + requestHeader.Topic + " topicConfig.readQueueNums: " + strconv.Itoa(topicConfig.ReadQueueNums)
		// TODO log.warn(errorInfo);
		response.Code = commonprotocol.SYSTEM_ERROR
		response.Remark = errorInfo
		return response
	}
	// 订阅关系处理
	subscriptionData := &heartbeat.SubscriptionData{}
	if hasSubscriptionFlag {
		var err error
		subscriptionData, err = filter.BuildSubscriptionData4Ponit(requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.Subscription)
		if err != nil {
			// TODO log.warn("parse the consumer's subscription[{}] failed, group: {}",
			// TODO 	requestHeader.getSubscription(),//
			// TODO 	requestHeader.getConsumerGroup());
			response.Code = commonprotocol.SUBSCRIPTION_PARSE_FAILED
			response.Remark = "parse the consumer's subscription failed"
			return response
		}
	} else {
		// 如果没有获取到维护的consumerGroup信息，则返回
		consumerGroupInfo := self.BrokerController.ConsumerManager.getConsumerGroupInfo(requestHeader.ConsumerGroup)
		if nil == consumerGroupInfo {
			// TODO	log.warn("the consumer's group info not exist, group: {}", requestHeader.getConsumerGroup());
			response.Code = commonprotocol.SUBSCRIPTION_NOT_EXIST
			response.Remark = "the consumer's group info not exist"
			return response
		}

		if !subscriptionGroupConfig.ConsumeBroadcastEnable && consumerGroupInfo.MessageModel == heartbeat.BROADCASTING {
			response.Code = commonprotocol.NO_PERMISSION
			response.Remark = "the consumer group[" + requestHeader.ConsumerGroup
			return response
		}

		subscriptionData = consumerGroupInfo.FindSubscriptionData(requestHeader.Topic)
		if nil == subscriptionData {
			// TODO log.warn("the consumer's subscription not exist, group: {}", requestHeader.getConsumerGroup());
			response.Code = commonprotocol.SUBSCRIPTION_NOT_EXIST
			response.Remark = "the consumer's subscription not exist"
			return response
		}

		// 判断Broker的订阅关系版本是否最新
		if subscriptionData.SubVersion < requestHeader.SubVersion {
			// TODO log.warn("the broker's subscription is not latest, group: {} {}",	requestHeader.getConsumerGroup(), subscriptionData.getSubString());
			response.Code = commonprotocol.SUBSCRIPTION_NOT_LATEST
			response.Remark = "the consumer's subscription not latestGetMessageResult"
			return response
		}
	}

	// TODO 	 this.brokerController.getMessageStore().getMessage(requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getQueueOffset(), requestHeader.getMaxMsgNums(), subscriptionData);
	getMessageResult := &stgstorelog.GetMessageResult{}
	if nil == getMessageResult {
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
			response.Code = (commonprotocol.SUCCESS)
			// 消息轨迹：记录客户端拉取的消息记录（不表示消费成功）
			if self.hasConsumeMessageHook() {
				// 执行hook
				context := mqtrace.ConsumeMessageContext{}
				context.ConsumerGroup = requestHeader.ConsumerGroup
				context.Topic = requestHeader.Topic
				// TODO context.ClientHost(RemotingHelper.parseChannelRemoteAddr(channel));
				context.StoreHost = self.BrokerController.GetBrokerAddr()
				context.QueueId = requestHeader.QueueId

				// TODO final SocketAddress storeHost =new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(), brokerController.getNettyServerConfig().getListenPort());
				// TODO	messageIds :=self.BrokerController.getMessageStore().getMessageIds(requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getQueueOffset(), requestHeader.getQueueOffset() + getMessageResult.getMessageCount(), storeHost);
				messageIds := make(map[string]int64)
				context.MessageIds = messageIds
				context.BodyLength = getMessageResult.BufferTotalSize / getMessageResult.GetMessageCount()
				// TODO this.executeConsumeMessageHookBefore(context);
			}
		case stgstorelog.MESSAGE_WAS_REMOVING:
			response.Code = commonprotocol.PULL_RETRY_IMMEDIATELY
			// 这两个返回值都表示服务器暂时没有这个队列，应该立刻将客户端Offset重置为0
		case stgstorelog.NO_MATCHED_LOGIC_QUEUE:
		case stgstorelog.NO_MESSAGE_IN_QUEUE:
			if 0 != requestHeader.QueueOffset {
				response.Code = commonprotocol.PULL_OFFSET_MOVED
				// TODO log
			} else {
				response.Code = commonprotocol.PULL_NOT_FOUND
			}
		case stgstorelog.NO_MATCHED_MESSAGE:
			response.Code = commonprotocol.PULL_RETRY_IMMEDIATELY
		case stgstorelog.OFFSET_FOUND_NULL:
			response.Code = commonprotocol.PULL_NOT_FOUND
		case stgstorelog.OFFSET_OVERFLOW_BADLY:
			response.Code = commonprotocol.PULL_OFFSET_MOVED
			// TODO log.info("the request offset: " + requestHeader.getQueueOffset()+ " over flow badly, broker max offset: " + getMessageResult.getMaxOffset()+ ", consumer: " + channel.remoteAddress());
		case stgstorelog.OFFSET_OVERFLOW_ONE:
			response.Code = commonprotocol.PULL_NOT_FOUND
		case stgstorelog.OFFSET_TOO_SMALL:
			response.Code = commonprotocol.PULL_OFFSET_MOVED
			// TODO log.info("the request offset: " + requestHeader.getQueueOffset()+ " too small, broker min offset: " + getMessageResult.getMinOffset()+ ", consumer: " + channel.remoteAddress());
		default:
		}
		switch response.Code {
		case commonprotocol.SUCCESS:
			// TODO  统计
			//this.brokerController.getBrokerStatsManager().incGroupGetNums(
			//	requestHeader.getConsumerGroup(), requestHeader.getTopic(),
			//	getMessageResult.getMessageCount());
			//
			//this.brokerController.getBrokerStatsManager().incGroupGetSize(
			//	requestHeader.getConsumerGroup(), requestHeader.getTopic(),
			//	getMessageResult.getBufferTotalSize());
			//
			//this.brokerController.getBrokerStatsManager().incBrokerGetNums(
			//	getMessageResult.getMessageCount());
			//
			//	FileRegion fileRegion =
			//	new ManyMessageTransfer(response.encodeHeader(getMessageResult
			//	.getBufferTotalSize()), getMessageResult);
			//	channel.writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
			//	public void operationComplete(ChannelFuture future) throws Exception {
			//	getMessageResult.release();
			//	if (!future.isSuccess()) {
			//	log.error(
			//	"transfer many message by pagecache failed, " + channel.remoteAddress(),
			//	future.cause());
			//	}
			//	}
			//	});

			response = nil
		case commonprotocol.PULL_NOT_FOUND:
			// 长轮询
			if brokerAllowSuspend && hasSuspendFlag {
				pollingTimeMills := suspendTimeoutMillisLong
				if !self.BrokerController.BrokerConfig.LongPollingEnable {
					pollingTimeMills = self.BrokerController.BrokerConfig.ShortPollingTimeMills
				}
				fmt.Println(pollingTimeMills)
				// pullRequest :=longpolling.NewPullRequest(request,pollingTimeMills,self.BrokerController.)
				//	new PullRequest(request, channel, pollingTimeMills, this.brokerController
				//.getMessageStore().now(), requestHeader.getQueueOffset());
				//this.brokerController.getPullRequestHoldService().suspendPullRequest(
				//	requestHeader.getTopic(), requestHeader.getQueueId(), pullRequest);
				//response = null;
			}
		case commonprotocol.PULL_RETRY_IMMEDIATELY:
		case commonprotocol.PULL_OFFSET_MOVED:
			//if (self.B.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE
			if self.BrokerController.BrokerConfig.OffsetCheckInSlave {

				mq := message.MessageQueue{
					Topic:      requestHeader.Topic,
					QueueId:    requestHeader.QueueId,
					BrokerName: self.BrokerController.BrokerConfig.BrokerName,
				}

				event := topic.OffsetMovedEvent{
					ConsumerGroup: requestHeader.ConsumerGroup,
					MessageQueue:  mq,
					OffsetRequest: requestHeader.QueueOffset,
					OffsetNew:     getMessageResult.NextBeginOffset,
				}

				self.generateOffsetMovedEvent(event)
			} else {
				responseHeader.SuggestWhichBrokerId = subscriptionGroupConfig.BrokerId
				response.Code = commonprotocol.PULL_RETRY_IMMEDIATELY
			}

		}
		// TODO
		// 存储Consumer消费进度
		storeOffsetEnable := brokerAllowSuspend; // 说明是首次调用，相对于长轮询通知
		storeOffsetEnable = storeOffsetEnable && hasCommitOffsetFlag; // 说明Consumer设置了标志位
		// TODO 	storeOffsetEnable = storeOffsetEnable // 只有Master支持存储offset && self.BrokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;

	} else {
		response.Code = commonprotocol.SYSTEM_ERROR
		response.Remark = "store getMessage return null"
	}

	// TODO 存储Consumer消费进度
	return response
}

func (self *PullMessageProcessor) hasConsumeMessageHook() bool {
	return self.ConsumeMessageHookList.Len() > 0
}

func (self *PullMessageProcessor) generateOffsetMovedEvent(event topic.OffsetMovedEvent) {
	// TODO
}

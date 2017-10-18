package process

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/listener"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/store"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/filter"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	set "github.com/deckarep/golang-set"
)

// DefaultMQPushConsumerImpl: push消费的实现
// Author: yintongqiang
// Since:  2017/8/10

type DefaultMQPushConsumerImpl struct {
	defaultMQPushConsumer             *DefaultMQPushConsumer
	serviceState                      stgcommon.ServiceState
	mQClientFactory                   *MQClientInstance
	pause                             bool
	rebalanceImpl                     RebalanceImpl
	messageListenerInner              listener.MessageListener
	pullAPIWrapper                    *PullAPIWrapper
	OffsetStore                       store.OffsetStore
	consumeMessageService             ConsumeMessageService
	consumeOrderly                    bool
	PullTimeDelayMillsWhenException   int
	PullTimeDelayMillsWhenSuspend     int
	BrokerSuspendMaxTimeMillis        int
	PullTimeDelayMillsWhenFlowControl int
	ConsumerTimeoutMillisWhenSuspend  int
	flowControlTimes1                 int64
	flowControlTimes2                 int64
}

func NewDefaultMQPushConsumerImpl(defaultMQPushConsumer *DefaultMQPushConsumer) *DefaultMQPushConsumerImpl {
	impl := &DefaultMQPushConsumerImpl{defaultMQPushConsumer: defaultMQPushConsumer, serviceState: stgcommon.CREATE_JUST,
		PullTimeDelayMillsWhenException: 3000, PullTimeDelayMillsWhenSuspend: 1000, BrokerSuspendMaxTimeMillis: 1000 * 15,
		PullTimeDelayMillsWhenFlowControl: 50, ConsumerTimeoutMillisWhenSuspend: 1000 * 30, flowControlTimes1: 0, flowControlTimes2: 0}
	impl.rebalanceImpl = NewRebalancePushImpl(impl)
	return impl
}

// pullMessage消息放到阻塞队列中
func (impl *DefaultMQPushConsumerImpl) pullMessage(pullRequest *consumer.PullRequest) {
	processQueue := pullRequest.ProcessQueue
	if processQueue.Dropped {
		logger.Infof("the pull request is droped.")
		return
	}
	pullRequest.ProcessQueue.LastPullTimestamp = time.Now().Unix() * 1000
	//logger.Info(pullRequest.ProcessQueue.ToString())
	if impl.serviceState != stgcommon.RUNNING {
		logger.Error("The consumer service state not OK")
		panic(errors.New("The consumer service state not OK"))
	}
	// 用于延迟消费
	if impl.pause {
		logger.Warnf("consumer was paused, execute pull request later. instanceName=%v", impl.defaultMQPushConsumer.clientConfig.InstanceName)
		impl.ExecutePullRequestLater(pullRequest, impl.PullTimeDelayMillsWhenSuspend)
		return
	}
	size := processQueue.MsgCount
	// 控流
	if size > impl.defaultMQPushConsumer.pullThresholdForQueue {
		impl.ExecutePullRequestLater(pullRequest, impl.PullTimeDelayMillsWhenFlowControl)
		impl.flowControlTimes1++
		if (impl.flowControlTimes1 % 1000) == 0 {
			logger.Warnf("the consumer message buffer is full, so do flow control, %v %v %v", size,
				pullRequest, impl.flowControlTimes1)
		}
		return
	}
	// 控流
	if !impl.consumeOrderly {
		if processQueue.GetMaxSpan() > impl.defaultMQPushConsumer.consumeConcurrentlyMaxSpan {
			impl.ExecutePullRequestLater(pullRequest, impl.PullTimeDelayMillsWhenFlowControl)
			impl.flowControlTimes2++
			if (impl.flowControlTimes2 % 1000) == 0 {
				logger.Warnf("the queue's messages, span too long, so do flow control, %v %v %v", processQueue.GetMaxSpan(),
					pullRequest, impl.flowControlTimes2)
			}
			return
		}
	}
	subData, _ := impl.rebalanceImpl.(*RebalancePushImpl).rebalanceImplExt.SubscriptionInner.Get(pullRequest.MessageQueue.Topic)
	if nil == subData {
		impl.ExecutePullRequestLater(pullRequest, impl.PullTimeDelayMillsWhenException)
		return
	}
	var pullCallBack PullCallback = &PullCallBackImpl{PullRequest: pullRequest, DefaultMQPushConsumerImpl: impl,
		SubscriptionData: subData.(*heartbeat.SubscriptionData), beginTimestamp: time.Now().Unix() * 1000}
	commitOffsetEnable := false
	var commitOffsetValue int64 = 0
	if impl.defaultMQPushConsumer.messageModel == heartbeat.CLUSTERING {
		commitOffsetValue = impl.OffsetStore.ReadOffset(pullRequest.MessageQueue, store.READ_FROM_MEMORY)
		if commitOffsetValue > 0 {
			commitOffsetEnable = true
		}
	}
	var subExpression string
	var classFilter bool = false
	sd, _ := impl.rebalanceImpl.(*RebalancePushImpl).rebalanceImplExt.SubscriptionInner.Get(pullRequest.MessageQueue.Topic)
	// todo class filter
	if sd != nil {
		subExpression = sd.(*heartbeat.SubscriptionData).SubString
	}
	sysFlag := sysflag.BuildSysFlag(commitOffsetEnable, true, !strings.EqualFold(subExpression, ""), classFilter)
	impl.pullAPIWrapper.PullKernelImpl(pullRequest.MessageQueue,
		subExpression,
		subData.(*heartbeat.SubscriptionData).SubVersion,
		pullRequest.NextOffset,
		impl.defaultMQPushConsumer.pullBatchSize,
		sysFlag,
		commitOffsetValue,
		impl.BrokerSuspendMaxTimeMillis,
		impl.ConsumerTimeoutMillisWhenSuspend,
		ASYNC,
		pullCallBack)
}

type PullCallBackImpl struct {
	beginTimestamp int64
	*heartbeat.SubscriptionData
	*consumer.PullRequest
	*DefaultMQPushConsumerImpl
}

func (backImpl *PullCallBackImpl) OnSuccess(pullResultExt *PullResultExt) {
	backImpl.pullAPIWrapper.processPullResult(backImpl.MessageQueue, pullResultExt, backImpl.SubscriptionData)
	pullResult := pullResultExt.PullResult
	switch pullResult.PullStatus {
	case consumer.FOUND:
		prevRequestOffset := backImpl.NextOffset
		backImpl.PullRequest.NextOffset = pullResult.NextBeginOffset
		//pullRT:=time.Now().Unix()*1000-backImpl.beginTimestamp
		var firstMsgOffset int64 = math.MaxInt64
		if len(pullResult.MsgFoundList) == 0 {
			backImpl.DefaultMQPushConsumerImpl.ExecutePullRequestImmediately(backImpl.PullRequest)
		} else {
			firstMsgOffset = pullResult.MsgFoundList[0].QueueOffset
			dispathToConsume := backImpl.ProcessQueue.PutMessage(pullResult.MsgFoundList)
			backImpl.consumeMessageService.SubmitConsumeRequest(pullResult.MsgFoundList, backImpl.ProcessQueue, backImpl.PullRequest.MessageQueue, dispathToConsume)
			if backImpl.DefaultMQPushConsumerImpl.defaultMQPushConsumer.pullInterval > 0 {
				backImpl.DefaultMQPushConsumerImpl.ExecutePullRequestLater(backImpl.PullRequest, int(backImpl.DefaultMQPushConsumerImpl.defaultMQPushConsumer.pullInterval))
			} else {
				backImpl.DefaultMQPushConsumerImpl.ExecutePullRequestImmediately(backImpl.PullRequest)
			}
		}
		if pullResult.NextBeginOffset < prevRequestOffset || firstMsgOffset < prevRequestOffset {
			logger.Warnf(
				"[BUG] pull message result maybe data wrong, nextBeginOffset: %v firstMsgOffset: %v prevRequestOffset: %v", //
				pullResult.NextBeginOffset, //
				firstMsgOffset,             //
				prevRequestOffset)
		}
	case consumer.NO_NEW_MSG:
		backImpl.NextOffset = pullResult.NextBeginOffset
		backImpl.DefaultMQPushConsumerImpl.correctTagsOffset(backImpl.PullRequest)
		backImpl.DefaultMQPushConsumerImpl.ExecutePullRequestImmediately(backImpl.PullRequest)
	case consumer.NO_MATCHED_MSG:
		backImpl.NextOffset = pullResult.NextBeginOffset
		backImpl.DefaultMQPushConsumerImpl.correctTagsOffset(backImpl.PullRequest)
		backImpl.DefaultMQPushConsumerImpl.ExecutePullRequestImmediately(backImpl.PullRequest)
	case consumer.OFFSET_ILLEGAL:
		backImpl.NextOffset = pullResult.NextBeginOffset
		backImpl.ProcessQueue.Dropped = true
		go func() {
			time.Sleep(time.Second * 10)
			backImpl.DefaultMQPushConsumerImpl.OffsetStore.UpdateOffset(backImpl.MessageQueue, backImpl.PullRequest.NextOffset, false)
			backImpl.DefaultMQPushConsumerImpl.OffsetStore.Persist(backImpl.MessageQueue)
			backImpl.DefaultMQPushConsumerImpl.rebalanceImpl.(*RebalancePushImpl).rebalanceImplExt.RemoveProcessQueue(backImpl.MessageQueue)
		}()

	}

}

func (pushConsumerImpl *DefaultMQPushConsumerImpl) correctTagsOffset(pullRequest *consumer.PullRequest) {
	if pullRequest.ProcessQueue.MsgCount == 0 {
		pushConsumerImpl.OffsetStore.UpdateOffset(pullRequest.MessageQueue, pullRequest.NextOffset, true)
	}
}

// 订阅topic和tag
func (impl *DefaultMQPushConsumerImpl) subscribe(topic string, subExpression string) {
	subscriptionData, _ := filter.BuildSubscriptionData(impl.defaultMQPushConsumer.consumerGroup, topic, subExpression)
	var pushImpl *RebalancePushImpl = impl.rebalanceImpl.(*RebalancePushImpl)
	pushImpl.rebalanceImplExt.SubscriptionInner.Put(topic, subscriptionData)
	if impl.mQClientFactory != nil {
		impl.mQClientFactory.SendHeartbeatToAllBrokerWithLock()
	}
}

// 注册监听器
func (pushConsumerImpl *DefaultMQPushConsumerImpl) registerMessageListener(messageListener listener.MessageListener) {
	pushConsumerImpl.messageListenerInner = messageListener
}

// 启动消费服务器
func (pushConsumerImpl *DefaultMQPushConsumerImpl) Start() {
	switch pushConsumerImpl.serviceState {
	case stgcommon.CREATE_JUST:
		pushConsumerImpl.serviceState = stgcommon.START_FAILED
		// 检查配置
		pushConsumerImpl.checkConfig()
		// 复制订阅信息
		pushConsumerImpl.copySubscription()
		if pushConsumerImpl.defaultMQPushConsumer.messageModel == heartbeat.CLUSTERING {
			pushConsumerImpl.defaultMQPushConsumer.clientConfig.ChangeInstanceNameToPID()
		}
		pushConsumerImpl.mQClientFactory = GetInstance().GetAndCreateMQClientInstance(pushConsumerImpl.defaultMQPushConsumer.clientConfig)

		var pushReImpl *RebalancePushImpl = pushConsumerImpl.rebalanceImpl.(*RebalancePushImpl)
		pushReImpl.rebalanceImplExt.ConsumerGroup = pushConsumerImpl.defaultMQPushConsumer.consumerGroup
		pushReImpl.rebalanceImplExt.MessageModel = pushConsumerImpl.defaultMQPushConsumer.messageModel
		pushReImpl.rebalanceImplExt.AllocateMessageQueueStrategy = pushConsumerImpl.defaultMQPushConsumer.allocateMessageQueueStrategy
		pushReImpl.rebalanceImplExt.MQClientFactory = pushConsumerImpl.mQClientFactory
		pushConsumerImpl.pullAPIWrapper = NewPullAPIWrapper(pushConsumerImpl.mQClientFactory,
			pushConsumerImpl.defaultMQPushConsumer.consumerGroup,
			pushConsumerImpl.defaultMQPushConsumer.unitMode)
		//todo registerFilterMessageHook
		if pushConsumerImpl.defaultMQPushConsumer.offsetStore != nil {
			pushConsumerImpl.OffsetStore = pushConsumerImpl.defaultMQPushConsumer.offsetStore
		} else {
			switch pushConsumerImpl.defaultMQPushConsumer.messageModel {
			case heartbeat.BROADCASTING:
				pushConsumerImpl.OffsetStore = NewLocalFileOffsetStore(pushConsumerImpl.mQClientFactory, pushConsumerImpl.defaultMQPushConsumer.consumerGroup)
			case heartbeat.CLUSTERING:
				pushConsumerImpl.OffsetStore = NewRemoteBrokerOffsetStore(pushConsumerImpl.mQClientFactory, pushConsumerImpl.defaultMQPushConsumer.consumerGroup)
			default:

			}
			// 本地存储，load才有用
			pushConsumerImpl.OffsetStore.Load()
			switch pushConsumerImpl.messageListenerInner.(type) {
			case consumer.MessageListenerConcurrently:
				pushConsumerImpl.consumeOrderly = false
				pushConsumerImpl.consumeMessageService = NewConsumeMessageConcurrentlyService(pushConsumerImpl, pushConsumerImpl.messageListenerInner.(consumer.MessageListenerConcurrently))
				//todo 顺序消费
			default:
				break
			}
			//启动拉取服务
			pushConsumerImpl.consumeMessageService.Start()
			// 注册consumer
			pushConsumerImpl.mQClientFactory.RegisterConsumer(pushConsumerImpl.defaultMQPushConsumer.consumerGroup, pushConsumerImpl)
			// 启动核心
			pushConsumerImpl.mQClientFactory.Start()
			pushConsumerImpl.serviceState = stgcommon.RUNNING
		}
	case stgcommon.RUNNING:
	case stgcommon.SHUTDOWN_ALREADY:
	case stgcommon.START_FAILED:
	default:
		break
	}
	pushConsumerImpl.updateTopicSubscribeInfoWhenSubscriptionChanged()
	pushConsumerImpl.mQClientFactory.SendHeartbeatToAllBrokerWithLock()
	pushConsumerImpl.mQClientFactory.rebalanceImmediately()
}

// 关闭
func (pushConsumerImpl *DefaultMQPushConsumerImpl) Shutdown() {
	switch pushConsumerImpl.serviceState {
	case stgcommon.CREATE_JUST:
	case stgcommon.RUNNING:
		pushConsumerImpl.consumeMessageService.Shutdown()
		pushConsumerImpl.PersistConsumerOffset()
		pushConsumerImpl.mQClientFactory.UnregisterConsumer(pushConsumerImpl.defaultMQPushConsumer.consumerGroup)
		pushConsumerImpl.mQClientFactory.Shutdown()
		logger.Infof("the consumer [%v] shutdown OK", pushConsumerImpl.defaultMQPushConsumer.consumerGroup)
		pushConsumerImpl.serviceState = stgcommon.SHUTDOWN_ALREADY
		pushConsumerImpl.rebalanceImpl.(*RebalancePushImpl).rebalanceImplExt.destroy()
	case stgcommon.SHUTDOWN_ALREADY:
	default:
	}
}

// 检查配置
func (pushConsumerImpl *DefaultMQPushConsumerImpl) checkConfig() {
	CheckGroup(pushConsumerImpl.defaultMQPushConsumer.consumerGroup)
	if strings.EqualFold("", pushConsumerImpl.defaultMQPushConsumer.consumerGroup) {
		panic("consumerGroup is null")
	}
	if strings.EqualFold(pushConsumerImpl.defaultMQPushConsumer.consumerGroup, stgcommon.DEFAULT_CONSUMER_GROUP) {
		panic("consumerGroup can not equal" + stgcommon.DEFAULT_CONSUMER_GROUP + ", please specify another one.")
	}
	if pushConsumerImpl.defaultMQPushConsumer.messageListener == nil {
		panic("messageListener is null")
	}
}

// 消费不了从新发送到队列
func (pushConsumerImpl *DefaultMQPushConsumerImpl) sendMessageBack(msg *message.MessageExt, delayLevel int, brokerName string) {
	var brokerAddr string
	if !strings.EqualFold(brokerName, "") {
		brokerAddr = pushConsumerImpl.mQClientFactory.FindBrokerAddressInPublish(brokerAddr)
	} else {
		brokerAddr = msg.BornHost
	}
	pushConsumerImpl.mQClientFactory.MQClientAPIImpl.consumerSendMessageBack(brokerAddr, msg, pushConsumerImpl.defaultMQPushConsumer.consumerGroup, delayLevel, 5000)
	defer func() {
		if e := recover(); e != nil {
			logger.Warnf("sendMessageBack Exception,%v ", pushConsumerImpl.defaultMQPushConsumer.consumerGroup)
			newMsg := &message.Message{Topic: stgcommon.GetRetryTopic(pushConsumerImpl.defaultMQPushConsumer.consumerGroup), Body: msg.Body}
			originMsgId := message.GetOriginMessageId(msg.Message)
			if strings.EqualFold(originMsgId, "") {
				message.SetOriginMessageId(newMsg, msg.MsgId)
			} else {
				message.SetOriginMessageId(newMsg, originMsgId)
			}
			newMsg.Flag = msg.Flag
			message.SetPropertiesMap(newMsg, msg.Properties)
			message.PutProperty(newMsg, message.PROPERTY_RETRY_TOPIC, msg.Topic)
			reTimes := msg.ReconsumeTimes + 1
			message.SetReconsumeTime(newMsg, strconv.Itoa(int(reTimes)))
			newMsg.PutProperty(message.PROPERTY_DELAY_TIME_LEVEL, strconv.Itoa(3+int(reTimes)))
			pushConsumerImpl.mQClientFactory.DefaultMQProducer.Send(newMsg)
		}
	}()
}

// 复制订阅信息
func (pushConsumerImpl *DefaultMQPushConsumerImpl) copySubscription() {
	sub := pushConsumerImpl.defaultMQPushConsumer.subscription
	if len(sub) > 0 {
		for topic, subString := range sub {
			subscriptionData, _ := filter.BuildSubscriptionData(pushConsumerImpl.defaultMQPushConsumer.consumerGroup, topic, subString)
			pushConsumerImpl.rebalanceImpl.(*RebalancePushImpl).rebalanceImplExt.SubscriptionInner.Put(topic, subscriptionData)
		}
	}
	if pushConsumerImpl.messageListenerInner == nil {
		pushConsumerImpl.messageListenerInner = pushConsumerImpl.defaultMQPushConsumer.messageListener
	}
	switch pushConsumerImpl.defaultMQPushConsumer.messageModel {
	case heartbeat.BROADCASTING:
	case heartbeat.CLUSTERING:
		retryTopic := stgcommon.GetRetryTopic(pushConsumerImpl.defaultMQPushConsumer.consumerGroup)
		subscriptionData, _ := filter.BuildSubscriptionData(pushConsumerImpl.defaultMQPushConsumer.consumerGroup, retryTopic, "*")
		logger.Infof("put retryTopic to SubscriptionInner. key=%s, value=%s", retryTopic, subscriptionData.ToString())
		pushConsumerImpl.rebalanceImpl.(*RebalancePushImpl).rebalanceImplExt.SubscriptionInner.Put(retryTopic, subscriptionData)
	}
}

// 当订阅信息改变时，更新订阅信息
func (pushConsumerImpl *DefaultMQPushConsumerImpl) updateTopicSubscribeInfoWhenSubscriptionChanged() {
	subTable := pushConsumerImpl.rebalanceImpl.(*RebalancePushImpl).rebalanceImplExt.SubscriptionInner
	if subTable != nil {
		logger.Infof("subscriptionInner size = %d", subTable.Size())
		for itor := subTable.Iterator(); itor.HasNext(); {
			topic, value, _ := ite.Next()
			logger.Infof("subscriptionInner info ----> %s, %s", topic.(string), value.(*heartbeat.SubscriptionData).ToString())
			pushConsumerImpl.mQClientFactory.UpdateTopicRouteInfoFromNameServerByTopic(topic.(string))
		}
	}
}

// 获取订阅信息
func (pushConsumerImpl *DefaultMQPushConsumerImpl) Subscriptions() set.Set {
	subSet := set.NewSet()
	for it := pushConsumerImpl.rebalanceImpl.(*RebalancePushImpl).rebalanceImplExt.SubscriptionInner.Iterator(); it.HasNext(); {
		_, v, _ := it.Next()
		subSet.Add(v)
	}
	return subSet
}

// 更新订阅信息
func (pushConsumerImpl *DefaultMQPushConsumerImpl) UpdateTopicSubscribeInfo(topic string, info set.Set) {

	sbInner := pushConsumerImpl.rebalanceImpl.(*RebalancePushImpl).rebalanceImplExt.SubscriptionInner

	if sbInner != nil {
		//todo ContainsKey 有bug
		subData, _ := sbInner.Get(topic)
		if subData != nil {
			pushConsumerImpl.rebalanceImpl.(*RebalancePushImpl).rebalanceImplExt.TopicSubscribeInfoTable.Put(topic, info)
		}
	}
}

func (pushConsumerImpl *DefaultMQPushConsumerImpl) GroupName() string {
	return pushConsumerImpl.defaultMQPushConsumer.consumerGroup
}

func (pushConsumerImpl *DefaultMQPushConsumerImpl) MessageModel() heartbeat.MessageModel {
	return pushConsumerImpl.defaultMQPushConsumer.messageModel
}

func (pushConsumerImpl *DefaultMQPushConsumerImpl) ConsumeType() heartbeat.ConsumeType {
	return heartbeat.CONSUME_PASSIVELY
}

func (pushConsumerImpl *DefaultMQPushConsumerImpl) ConsumeFromWhere() heartbeat.ConsumeFromWhere {
	return pushConsumerImpl.defaultMQPushConsumer.consumeFromWhere
}

func (pushConsumerImpl *DefaultMQPushConsumerImpl) IsUnitMode() bool {
	return pushConsumerImpl.defaultMQPushConsumer.unitMode
}

// 立即执行pull请求
func (pushConsumerImpl *DefaultMQPushConsumerImpl) ExecutePullRequestImmediately(pullRequest *consumer.PullRequest) {
	pushConsumerImpl.mQClientFactory.PullMessageService.ExecutePullRequestImmediately(pullRequest)
}

// 延迟执行pull请求
func (pushConsumerImpl *DefaultMQPushConsumerImpl) ExecutePullRequestLater(pullRequest *consumer.PullRequest, timeDelay int) {
	pushConsumerImpl.mQClientFactory.PullMessageService.ExecutePullRequestLater(pullRequest, timeDelay)
}

// 执行负载
func (pushConsumerImpl *DefaultMQPushConsumerImpl) DoRebalance() {
	pushConsumerImpl.rebalanceImpl.(*RebalancePushImpl).rebalanceImplExt.doRebalance()
}

// 持久化消费offset
func (pushConsumerImpl *DefaultMQPushConsumerImpl) PersistConsumerOffset() {
	if pushConsumerImpl.serviceState != stgcommon.RUNNING {
		panic(errors.New("The consumer service state not OK"))
	}
	storeSet := set.NewSet()
	for ite := pushConsumerImpl.rebalanceImpl.(*RebalancePushImpl).rebalanceImplExt.ProcessQueueTable.Iterator(); ite.HasNext(); {
		k, _, _ := ite.Next()
		storeSet.Add(k)
	}
	pushConsumerImpl.OffsetStore.PersistAll(storeSet)
}

func (pushConsumerImpl *DefaultMQPushConsumerImpl) IsSubscribeTopicNeedUpdate(topic string) bool {
	sbInner := pushConsumerImpl.rebalanceImpl.(*RebalancePushImpl).rebalanceImplExt.SubscriptionInner
	info := pushConsumerImpl.rebalanceImpl.(*RebalancePushImpl).rebalanceImplExt.TopicSubscribeInfoTable
	//ok, _ := sbInner.ContainsKey(topic)
	ok, _ := sbInner.Get(topic)
	if ok == nil {
		//flag, _ := info.ContainsKey(topic)
		//return !flag
		flag, _ := info.Get(topic)
		return flag == nil
	}
	return false
}

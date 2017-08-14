package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"time"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"errors"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/listener"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgcommon/filter"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/store"
	set "github.com/deckarep/golang-set"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"strings"
)
// DefaultMQPushConsumerImpl: push消费的实现
// Author: yintongqiang
// Since:  2017/8/10

type DefaultMQPushConsumerImpl struct {
	defaultMQPushConsumer            *DefaultMQPushConsumer
	serviceState                     stgcommon.ServiceState
	mQClientFactory                  *MQClientInstance
	pause                            bool
	rebalanceImpl                    RebalanceImpl
	messageListenerInner             listener.MessageListener
	pullAPIWrapper                   *PullAPIWrapper
	OffsetStore                      store.OffsetStore
	consumeMessageService            ConsumeMessageService
	consumeOrderly                   bool
	PullTimeDelayMillsWhenException  int
	BrokerSuspendMaxTimeMillis       int
	ConsumerTimeoutMillisWhenSuspend int
}

func NewDefaultMQPushConsumerImpl(defaultMQPushConsumer *DefaultMQPushConsumer) *DefaultMQPushConsumerImpl {
	impl := &DefaultMQPushConsumerImpl{defaultMQPushConsumer:defaultMQPushConsumer, serviceState:stgcommon.CREATE_JUST,
		PullTimeDelayMillsWhenException:3000, BrokerSuspendMaxTimeMillis:1000 * 15, ConsumerTimeoutMillisWhenSuspend:1000 * 30}
	impl.rebalanceImpl = NewRebalancePushImpl(impl)
	return impl
}
// pullMessage消息放到阻塞队列中
func (impl*DefaultMQPushConsumerImpl)pullMessage(pullRequest consumer.PullRequest) {
	processQueue := pullRequest.ProcessQueue
	if processQueue.Dropped {
		logger.Info("the pull request is droped.")
		return
	}
	pullRequest.ProcessQueue.LastPullTimestamp = time.Now().Unix() * 1000
	if impl.serviceState != stgcommon.RUNNING {
		logger.Error("The consumer service state not OK")
		panic(errors.New("The consumer service state not OK"))
	}
	// todo 后续添加
	if impl.pause {

	}
	size := processQueue.MsgCount
	// todo 控流后续添加
	if size > impl.defaultMQPushConsumer.pullThresholdForQueue {

	}
	// todo 控流后续添加
	if !impl.consumeOrderly {

	}
	subData, _ := impl.rebalanceImpl.(RebalancePushImpl).rebalanceImplExt.SubscriptionInner.Get(pullRequest.MessageQueue.Topic)

	if nil == subData {
		impl.ExecutePullRequestLater(pullRequest, impl.PullTimeDelayMillsWhenException)
		return
	}
	var pullCallBack consumer.PullCallback = &PullCallBackImpl{PullRequest:pullRequest, DefaultMQPushConsumerImpl:impl,
		SubscriptionData:subData.(heartbeat.SubscriptionData),beginTimestamp:time.Now().Unix()*1000}
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
	sd, _ := impl.rebalanceImpl.(RebalancePushImpl).rebalanceImplExt.SubscriptionInner.Get(pullRequest.MessageQueue.Topic)
	// todo class filter
	if sd != nil {

	}
	sysFlag := sysflag.BuildSysFlag(commitOffsetEnable, true, !strings.EqualFold(subExpression, ""), classFilter)
	impl.pullAPIWrapper.PullKernelImpl(pullRequest.MessageQueue,
		subExpression,
		subData.(heartbeat.SubscriptionData).SubVersion,
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
	heartbeat.SubscriptionData
	consumer.PullRequest
	*DefaultMQPushConsumerImpl
}

func (backImpl PullCallBackImpl) OnSuccess(pullResult consumer.PullResult) {
	pullResult = backImpl.pullAPIWrapper.processPullResult(backImpl.MessageQueue, pullResult, backImpl.SubscriptionData)
	switch pullResult.PullStatus {
	case consumer.FOUND:
		//prevRequestOffset := backImpl.NextOffset
		backImpl.PullRequest.NextOffset=pullResult.NextBeginOffset
		//pullRT:=time.Now().Unix()*1000-backImpl.beginTimestamp
		if len(pullResult.MsgFoundList)==0{
			backImpl.DefaultMQPushConsumerImpl.ExecutePullRequestImmediately(backImpl.PullRequest)
		}else{
			dispathToConsume:=backImpl.ProcessQueue.PutMessage(pullResult.MsgFoundList)
			backImpl.consumeMessageService.SubmitConsumeRequest(pullResult.MsgFoundList,backImpl.ProcessQueue,backImpl.PullRequest.MessageQueue,dispathToConsume)
		}

	}

}

// 订阅topic和tag
func (impl *DefaultMQPushConsumerImpl) subscribe(topic string, subExpression string) {
	subscriptionData := filter.BuildSubscriptionData(impl.defaultMQPushConsumer.consumerGroup, topic, subExpression)
	var pushImpl RebalancePushImpl = impl.rebalanceImpl.(RebalancePushImpl)
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

		var pushReImpl RebalancePushImpl = pushConsumerImpl.rebalanceImpl.(RebalancePushImpl)
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
			//todo 本地存储
			case heartbeat.CLUSTERING:
				pushConsumerImpl.OffsetStore = NewRemoteBrokerOffsetStore(pushConsumerImpl.mQClientFactory, pushConsumerImpl.defaultMQPushConsumer.consumerGroup)
			default:
				break

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
	default:break
	}
	pushConsumerImpl.updateTopicSubscribeInfoWhenSubscriptionChanged()
	pushConsumerImpl.mQClientFactory.SendHeartbeatToAllBrokerWithLock()
	pushConsumerImpl.mQClientFactory.rebalanceImmediately()
}

// 检查配置
func (pushConsumerImpl *DefaultMQPushConsumerImpl)checkConfig() {

}

// 复制订阅信息
func (pushConsumerImpl *DefaultMQPushConsumerImpl)copySubscription() {

}

// 当订阅信息改变时，更新订阅信息
func (pushConsumerImpl *DefaultMQPushConsumerImpl)updateTopicSubscribeInfoWhenSubscriptionChanged() {

}


// 获取订阅信息
func (pushConsumerImpl *DefaultMQPushConsumerImpl)Subscriptions() set.Set {
	subSet := set.NewSet()

	for it := pushConsumerImpl.rebalanceImpl.(RebalancePushImpl).rebalanceImplExt.SubscriptionInner.Iterator(); it.HasNext(); {
		_, v, _ := it.Next()
		subSet.Add(v)
	}
	return subSet
}
// 更新订阅信息
func (pushConsumerImpl *DefaultMQPushConsumerImpl)UpdateTopicSubscribeInfo(topic string, info set.Set) {

	sbInner := pushConsumerImpl.rebalanceImpl.(RebalancePushImpl).rebalanceImplExt.SubscriptionInner
	if sbInner != nil {
		ok, _ := sbInner.ContainsKey(topic)
		if ok {
			pushConsumerImpl.rebalanceImpl.(RebalancePushImpl).rebalanceImplExt.TopicSubscribeInfoTable.Put(topic, info)
		}
	}
}

func (pushConsumerImpl *DefaultMQPushConsumerImpl)GroupName() string {
	return pushConsumerImpl.defaultMQPushConsumer.consumerGroup
}

func (pushConsumerImpl *DefaultMQPushConsumerImpl)MessageModel() heartbeat.MessageModel {
	return pushConsumerImpl.defaultMQPushConsumer.messageModel
}

func (pushConsumerImpl *DefaultMQPushConsumerImpl)ConsumeType() heartbeat.ConsumeType {
	return heartbeat.CONSUME_PASSIVELY
}

func (pushConsumerImpl *DefaultMQPushConsumerImpl)ConsumeFromWhere() heartbeat.ConsumeFromWhere {
	return pushConsumerImpl.defaultMQPushConsumer.consumeFromWhere
}

func (pushConsumerImpl *DefaultMQPushConsumerImpl)IsUnitMode() bool {
	return pushConsumerImpl.defaultMQPushConsumer.unitMode
}

func (pushConsumerImpl *DefaultMQPushConsumerImpl)ExecutePullRequestImmediately(pullRequest consumer.PullRequest) {
	pushConsumerImpl.mQClientFactory.PullMessageService.ExecutePullRequestImmediately(pullRequest)
}
func (pushConsumerImpl *DefaultMQPushConsumerImpl)ExecutePullRequestLater(pullRequest consumer.PullRequest, timeDelay int) {
	pushConsumerImpl.mQClientFactory.PullMessageService.ExecutePullRequestLater(pullRequest, timeDelay)
}

func (pushConsumerImpl *DefaultMQPushConsumerImpl)PersistConsumerOffset() {
	if pushConsumerImpl.serviceState != stgcommon.RUNNING {
		panic(errors.New("The consumer service state not OK"))
	}
	storeSet := set.NewSet()
	for ite := pushConsumerImpl.rebalanceImpl.(RebalancePushImpl).rebalanceImplExt.ProcessQueueTable.Iterator(); ite.HasNext(); {
		k, _, _ := ite.Next()
		storeSet.Add(k)
	}
	pushConsumerImpl.OffsetStore.PersistAll(storeSet)
}

func (pushConsumerImpl *DefaultMQPushConsumerImpl)IsSubscribeTopicNeedUpdate(topic string) bool {
	sbInner := pushConsumerImpl.rebalanceImpl.(RebalancePushImpl).rebalanceImplExt.SubscriptionInner
	info := pushConsumerImpl.rebalanceImpl.(RebalancePushImpl).rebalanceImplExt.TopicSubscribeInfoTable
	ok, _ := sbInner.ContainsKey(topic)
	if ok {
		flag, _ := info.ContainsKey(topic)
		return !flag
	}
	return false
}

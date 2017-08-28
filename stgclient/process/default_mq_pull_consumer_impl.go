package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/store"
	set "github.com/deckarep/golang-set"
	"time"
	"git.oschina.net/cloudzone/smartgo/stgcommon/filter"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"errors"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"strings"
)
// DefaultMQPullConsumerImpl: 拉取下线实现
// Author: yintongqiang
// Since:  2017/8/17

type DefaultMQPullConsumerImpl struct {
	defaultMQPullConsumer  *DefaultMQPullConsumer
	serviceState           stgcommon.ServiceState
	mQClientFactory        *MQClientInstance
	pullAPIWrapper         *PullAPIWrapper
	OffsetStore            store.OffsetStore
	RebalanceImpl          RebalanceImpl
	consumerStartTimestamp int64
}

func NewDefaultMQPullConsumerImpl(defaultMQPullConsumer *DefaultMQPullConsumer) *DefaultMQPullConsumerImpl {
	impl := &DefaultMQPullConsumerImpl{defaultMQPullConsumer:defaultMQPullConsumer,
		consumerStartTimestamp:time.Now().Unix() * 1000, serviceState:stgcommon.CREATE_JUST}
	impl.RebalanceImpl = NewRebalancePullImpl(impl)
	return impl
}

func (pullImpl*DefaultMQPullConsumerImpl)makeSureStateOK() {
	if pullImpl.serviceState != stgcommon.RUNNING {
		panic("The consumer service state not OK")
	}
}

func (pullImpl*DefaultMQPullConsumerImpl)shutdown() {
	switch pullImpl.serviceState {
	case stgcommon.CREATE_JUST:
	case stgcommon.RUNNING:
		pullImpl.PersistConsumerOffset()
		pullImpl.mQClientFactory.UnregisterConsumer(pullImpl.defaultMQPullConsumer.consumerGroup)
		pullImpl.mQClientFactory.Shutdown()
		logger.Infof("the consumer [%v] shutdown OK", pullImpl.defaultMQPullConsumer.consumerGroup)
		pullImpl.serviceState = stgcommon.SHUTDOWN_ALREADY
	case stgcommon.SHUTDOWN_ALREADY:
	default:

	}
}

func (pullImpl*DefaultMQPullConsumerImpl)Start() {
	switch pullImpl.serviceState {
	case stgcommon.CREATE_JUST:
		pullImpl.serviceState = stgcommon.START_FAILED
		// 检查配置
		pullImpl.checkConfig()
		// 复制订阅信息
		pullImpl.copySubscription()
		if pullImpl.defaultMQPullConsumer.messageModel == heartbeat.CLUSTERING {
			pullImpl.defaultMQPullConsumer.clientConfig.ChangeInstanceNameToPID()
		}
		pullImpl.mQClientFactory = GetInstance().GetAndCreateMQClientInstance(pullImpl.defaultMQPullConsumer.clientConfig)
		var pullReImpl *RebalancePullImpl = pullImpl.RebalanceImpl.(*RebalancePullImpl)
		pullReImpl.ConsumerGroup = pullImpl.defaultMQPullConsumer.consumerGroup
		pullReImpl.MessageModel = pullImpl.defaultMQPullConsumer.messageModel
		pullReImpl.AllocateMessageQueueStrategy = pullImpl.defaultMQPullConsumer.allocateMessageQueueStrategy
		pullReImpl.MQClientFactory = pullImpl.mQClientFactory
		pullImpl.pullAPIWrapper = NewPullAPIWrapper(pullImpl.mQClientFactory,
			pullImpl.defaultMQPullConsumer.consumerGroup,
			pullImpl.defaultMQPullConsumer.unitMode)
		//todo registerFilterMessageHook
		if pullImpl.defaultMQPullConsumer.offsetStore != nil {
			pullImpl.OffsetStore = pullImpl.defaultMQPullConsumer.offsetStore
		} else {
			switch pullImpl.defaultMQPullConsumer.messageModel {
			case heartbeat.BROADCASTING:
				pullImpl.OffsetStore = NewLocalFileOffsetStore(pullImpl.mQClientFactory, pullImpl.defaultMQPullConsumer.consumerGroup)
			case heartbeat.CLUSTERING:
				pullImpl.OffsetStore = NewRemoteBrokerOffsetStore(pullImpl.mQClientFactory, pullImpl.defaultMQPullConsumer.consumerGroup)
			default:

			}
		}
		// 本地存储，load才有用
		pullImpl.OffsetStore.Load()
		// 注册consumer
		pullImpl.mQClientFactory.RegisterConsumer(pullImpl.defaultMQPullConsumer.consumerGroup, pullImpl)
		// 启动核心
		pullImpl.mQClientFactory.Start()
		logger.Infof("the consumer [%v] start OK", pullImpl.defaultMQPullConsumer.consumerGroup);
		pullImpl.serviceState = stgcommon.RUNNING
	case stgcommon.RUNNING:
	case stgcommon.SHUTDOWN_ALREADY:
		panic("The PullConsumer service state not OK, maybe started once")
	case stgcommon.START_FAILED:
	default:
	}

}

func (pullImpl*DefaultMQPullConsumerImpl)checkConfig() {
	CheckGroup(pullImpl.defaultMQPullConsumer.consumerGroup)
	if strings.EqualFold("", pullImpl.defaultMQPullConsumer.consumerGroup) {
		panic("consumerGroup is null")
	}
	if strings.EqualFold(pullImpl.defaultMQPullConsumer.consumerGroup, stgcommon.DEFAULT_CONSUMER_GROUP) {
		panic("consumerGroup can not equal" + stgcommon.DEFAULT_CONSUMER_GROUP + ", please specify another one.")
	}
}

func (pullImpl*DefaultMQPullConsumerImpl)copySubscription() {
	//todo registerTopics 一直为空
}

func (pullImpl *DefaultMQPullConsumerImpl)ConsumeFromWhere() heartbeat.ConsumeFromWhere {
	return heartbeat.CONSUME_FROM_LAST_OFFSET
}

// 获取订阅信息
func (pullImpl *DefaultMQPullConsumerImpl)Subscriptions() set.Set {
	subSet := set.NewSet()
	for topic := range pullImpl.defaultMQPullConsumer.registerTopics.Iterator().C {
		if topic != nil {
			subData, _ := filter.BuildSubscriptionData(pullImpl.defaultMQPullConsumer.consumerGroup, topic.(string), "*")
			subSet.Add(subData)
		}
	}
	return subSet
}

// 当订阅信息改变时，更新订阅信息
func (pullImpl *DefaultMQPullConsumerImpl)UpdateTopicSubscribeInfo(topic string, info set.Set) {
	sbInner := pullImpl.RebalanceImpl.(*RebalancePullImpl).SubscriptionInner
	if sbInner != nil {
		//todo ContainsKey 有bug
		subData, _ := sbInner.Get(topic)
		if subData != nil {
			pullImpl.RebalanceImpl.(*RebalancePullImpl).TopicSubscribeInfoTable.Put(topic, info)
		}
	}
}

func (pullImpl *DefaultMQPullConsumerImpl)GroupName() string {
	return pullImpl.defaultMQPullConsumer.consumerGroup
}

func (pullImpl *DefaultMQPullConsumerImpl)MessageModel() heartbeat.MessageModel {
	return pullImpl.defaultMQPullConsumer.messageModel
}

func (pullImpl *DefaultMQPullConsumerImpl)ConsumeType() heartbeat.ConsumeType {
	return heartbeat.CONSUME_ACTIVELY
}

func (pullImpl *DefaultMQPullConsumerImpl)IsUnitMode() bool {
	return pullImpl.defaultMQPullConsumer.unitMode
}

func (pullImpl *DefaultMQPullConsumerImpl)IsSubscribeTopicNeedUpdate(topic string) bool {
	sbInner := pullImpl.RebalanceImpl.(*RebalancePullImpl).SubscriptionInner
	if sbInner != nil {
		//todo ContainsKey 有bug
		subData, _ := sbInner.Get(topic)
		if subData != nil {
			v, _ := pullImpl.RebalanceImpl.(*RebalancePullImpl).TopicSubscribeInfoTable.Get(topic)
			return v == nil
		}
	}
	return false
}

func (pullImpl *DefaultMQPullConsumerImpl)PersistConsumerOffset() {
	if pullImpl.serviceState != stgcommon.RUNNING {
		panic(errors.New("The consumer service state not OK"))
	}
	storeSet := set.NewSet()
	for ite := pullImpl.RebalanceImpl.(*RebalancePullImpl).ProcessQueueTable.Iterator(); ite.HasNext(); {
		k, _, _ := ite.Next()
		storeSet.Add(k)
	}
	pullImpl.OffsetStore.PersistAll(storeSet)
}

func (pullImpl *DefaultMQPullConsumerImpl)DoRebalance() {
	pullImpl.RebalanceImpl.(*RebalancePullImpl).doRebalance()
}

func (pullImpl *DefaultMQPullConsumerImpl)fetchSubscribeMessageQueues(topic string) []*message.MessageQueue {
	pullImpl.makeSureStateOK()
	return pullImpl.mQClientFactory.MQAdminImpl.FetchSubscribeMessageQueues(topic)
}

func (pullImpl*DefaultMQPullConsumerImpl)pull(mq *message.MessageQueue, subExpression string, offset int64, maxNums int) (*consumer.PullResult,error) {
	return pullImpl.pullSyncImpl(mq, subExpression, offset, maxNums, false, pullImpl.defaultMQPullConsumer.consumerPullTimeoutMillis)
}

func (pullImpl*DefaultMQPullConsumerImpl)pullSyncImpl(mq *message.MessageQueue, subExpression string, offset int64, maxNums int, block bool, timeout int) (*consumer.PullResult,error) {
	pullImpl.makeSureStateOK()
	if offset < 0 {
		panic("offset < 0")
	}
	if maxNums <= 0 {
		panic("maxNums <= 0")
	}
	pullImpl.subscriptionAutomatically(mq.Topic)
	sysFlag := sysflag.BuildSysFlag(false, block, true, false)
	subData, _ := filter.BuildSubscriptionData(pullImpl.defaultMQPullConsumer.consumerGroup, mq.Topic, subExpression)
	var timeoutMillis int
	if block {
		timeoutMillis = pullImpl.defaultMQPullConsumer.consumerTimeoutMillisWhenSuspend
	} else {
		timeoutMillis = timeout
	}
	pullResultExt := pullImpl.pullAPIWrapper.PullKernelImpl(mq, subData.SubString, 0, offset, maxNums, sysFlag, 0,
		pullImpl.defaultMQPullConsumer.brokerSuspendMaxTimeMillis, timeoutMillis, SYNC, nil)
	if pullResultExt!=nil {
		return pullImpl.pullAPIWrapper.processPullResult(mq, pullResultExt, subData).PullResult,nil
	}
	return nil,errors.New("pullResult is nil")
}

func (pullImpl*DefaultMQPullConsumerImpl)subscriptionAutomatically(topic string) {
	tv, _ := pullImpl.RebalanceImpl.(*RebalancePullImpl).SubscriptionInner.Get(topic)
	if tv != nil {
		subData, _ := filter.BuildSubscriptionData(pullImpl.defaultMQPullConsumer.consumerGroup, topic, "*")
		pullImpl.RebalanceImpl.(*RebalancePullImpl).SubscriptionInner.PutIfAbsent(topic, subData)
	}
}
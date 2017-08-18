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
			//todo 本地存储
			case heartbeat.CLUSTERING:
				pullImpl.OffsetStore = NewRemoteBrokerOffsetStore(pullImpl.mQClientFactory, pullImpl.defaultMQPullConsumer.consumerGroup)
			default:
				break

			}
			// 本地存储，load才有用
			pullImpl.OffsetStore.Load()
			// 注册consumer
			pullImpl.mQClientFactory.RegisterConsumer(pullImpl.defaultMQPullConsumer.consumerGroup, pullImpl)
			// 启动核心
			pullImpl.mQClientFactory.Start()
			logger.Info("the consumer [%v] start OK", pullImpl.defaultMQPullConsumer.consumerGroup);
			pullImpl.serviceState = stgcommon.RUNNING
		}
	case stgcommon.RUNNING:
	case stgcommon.SHUTDOWN_ALREADY:
		panic("The PullConsumer service state not OK, maybe started once")
	case stgcommon.START_FAILED:
	default:break
	}

}

func (pullImpl*DefaultMQPullConsumerImpl)checkConfig() {

}

func (pullImpl*DefaultMQPullConsumerImpl)copySubscription() {

}

func (pullImpl *DefaultMQPullConsumerImpl)ConsumeFromWhere() heartbeat.ConsumeFromWhere {
	return heartbeat.CONSUME_FROM_LAST_OFFSET
}

// 获取订阅信息
func (pullImpl *DefaultMQPullConsumerImpl)Subscriptions() set.Set {
	subSet := set.NewSet()
	for topic := range pullImpl.defaultMQPullConsumer.registerTopics.Iterator().C {
		if topic != nil {
			subData := filter.BuildSubscriptionData(pullImpl.defaultMQPullConsumer.consumerGroup, topic.(string), "*")
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
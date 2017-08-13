package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	set "github.com/deckarep/golang-set"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/store"
)
// RebalancePushImpl: push负载实现类
// Author: yintongqiang
// Since:  2017/8/11

type RebalancePushImpl struct {
	defaultMQPushConsumerImpl *DefaultMQPushConsumerImpl
	rebalanceImplExt          RebalanceImplExt
}

func NewRebalancePushImpl(defaultMQPushConsumerImpl *DefaultMQPushConsumerImpl) RebalancePushImpl {
	rebalanceImpl:=RebalancePushImpl{defaultMQPushConsumerImpl:defaultMQPushConsumerImpl}
	rebalanceImpl.rebalanceImplExt=NewRebalanceImplExt(rebalanceImpl)
	return rebalanceImpl
}

func (pushImpl RebalancePushImpl)DispatchPullRequest(pullRequestList []consumer.PullRequest) {

}

func (pushImpl RebalancePushImpl)ConsumeType() heartbeat.ConsumeType {
	return heartbeat.CONSUME_PASSIVELY
}

func (pushImpl RebalancePushImpl)MessageQueueChanged(topic string, mqAll set.Set, mqDivided set.Set) {
}

func (pushImpl RebalancePushImpl)RemoveUnnecessaryMessageQueue(mq message.MessageQueue, pq consumer.ProcessQueue) bool {
	pushImpl.defaultMQPushConsumerImpl.OffsetStore.Persist(mq)
	pushImpl.defaultMQPushConsumerImpl.OffsetStore.RemoveOffset(mq)
	//todo 各种lock
	return true
}

func (pushImpl RebalancePushImpl)ComputePullFromWhere(mq message.MessageQueue) int64 {
	var result int64=-1
	consumeFromWhere:=pushImpl.defaultMQPushConsumerImpl.ConsumeFromWhere()
	offsetStore:=pushImpl.defaultMQPushConsumerImpl.OffsetStore
	switch consumeFromWhere {
	case heartbeat.CONSUME_FROM_LAST_OFFSET:
		/*lastOffset:=*/offsetStore.ReadOffset(mq,store.READ_FROM_STORE)
	}
	return result
}
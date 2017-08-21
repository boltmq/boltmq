package process

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	set "github.com/deckarep/golang-set"
)
// RebalancePullImpl: 拉消息负载
// Author: yintongqiang
// Since:  2017/8/18

type RebalancePullImpl struct {
	defaultMQPullConsumerImpl *DefaultMQPullConsumerImpl
	*RebalanceImplExt
}

func NewRebalancePullImpl(defaultMQPullConsumerImpl *DefaultMQPullConsumerImpl) *RebalancePullImpl {
	rebalanceImpl := &RebalancePullImpl{defaultMQPullConsumerImpl:defaultMQPullConsumerImpl}
	rebalanceImpl.RebalanceImplExt = NewRebalanceImplExt(rebalanceImpl)
	return rebalanceImpl
}

func (pullImpl *RebalancePullImpl)DispatchPullRequest(pullRequestList []*consumer.PullRequest) {

}

func (pullImpl *RebalancePullImpl)ConsumeType() heartbeat.ConsumeType {
	return heartbeat.CONSUME_ACTIVELY
}

func (pullImpl *RebalancePullImpl)MessageQueueChanged(topic string, mqAll set.Set, mqDivided set.Set) {
	//todo MessageQueueListener
}

func (pullImpl *RebalancePullImpl)RemoveUnnecessaryMessageQueue(mq *message.MessageQueue, pq *consumer.ProcessQueue) bool {
	pullImpl.defaultMQPullConsumerImpl.OffsetStore.Persist(mq)
	pullImpl.defaultMQPullConsumerImpl.OffsetStore.RemoveOffset(mq)
	return true
}

func (pullImpl *RebalancePullImpl)ComputePullFromWhere(mq *message.MessageQueue) int64 {
	return 0
}
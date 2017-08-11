package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	set "github.com/deckarep/golang-set"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
)
// RebalancePushImpl: push负载实现类
// Author: yintongqiang
// Since:  2017/8/11

type RebalancePushImpl struct {
	defaultMQPushConsumerImpl *DefaultMQPushConsumerImpl
	rebalanceImplExt          RebalanceImplExt
}

func NewRebalancePushImpl(defaultMQPushConsumerImpl *DefaultMQPushConsumerImpl) RebalancePushImpl {
	return RebalancePushImpl{defaultMQPushConsumerImpl:defaultMQPushConsumerImpl,rebalanceImplExt:NewRebalanceImplExt()}
}

func (pushImpl RebalancePushImpl)DispatchPullRequest(pullRequestList []consumer.PullRequest) {

}

func (pushImpl RebalancePushImpl)ConsumeType() heartbeat.ConsumeType {
	return heartbeat.CONSUME_PASSIVELY
}

func (pushImpl RebalancePushImpl)MessageQueueChanged(topic string, mqAll set.Set, mqDivided set.Set) {
}

func (pushImpl RebalancePushImpl)RemoveUnnecessaryMessageQueue(mq message.MessageQueue, pq consumer.ProcessQueue) bool {
	return false
}

func (pushImpl RebalancePushImpl)ComputePullFromWhere(mq message.MessageQueue) int64 {
	return 1
}
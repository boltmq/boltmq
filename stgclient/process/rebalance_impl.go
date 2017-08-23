package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	set "github.com/deckarep/golang-set"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/rebalance"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"strings"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"sort"
)
// RebalanceImpl: rebalance接口
// Author: yintongqiang
// Since:  2017/8/11


type RebalanceImpl interface {
	ConsumeType() heartbeat.ConsumeType
	MessageQueueChanged(topic string, mqAll set.Set, mqDivided set.Set)
	RemoveUnnecessaryMessageQueue(mq *message.MessageQueue, pq *consumer.ProcessQueue) bool
	DispatchPullRequest(pullRequestList []*consumer.PullRequest)
	ComputePullFromWhere(mq *message.MessageQueue) int64
}
// RebalanceImplExt: 接口基础属性
// Author: yintongqiang
// Since:  2017/8/11

type RebalanceImplExt  struct {
	RebalanceImpl                RebalanceImpl
	ProcessQueueTable            *sync.Map //*MessageQueue, *ProcessQueue
	TopicSubscribeInfoTable      *sync.Map // topic  Set<*MessageQueue>
	SubscriptionInner            *sync.Map //topic, *SubscriptionData
	ConsumerGroup                string
	MessageModel                 heartbeat.MessageModel
	AllocateMessageQueueStrategy rebalance.AllocateMessageQueueStrategy
	MQClientFactory              *MQClientInstance
}

func NewRebalanceImplExt(rebalanceImpl RebalanceImpl) *RebalanceImplExt {
	return &RebalanceImplExt{
		RebalanceImpl:rebalanceImpl,
		ProcessQueueTable:sync.NewMap(),
		TopicSubscribeInfoTable:sync.NewMap(),
		SubscriptionInner:sync.NewMap()}
}

func (ext *RebalanceImplExt)doRebalance() {
	for ite := ext.SubscriptionInner.Iterator(); ite.HasNext(); {
		k, _, _ := ite.Next()
		topic := k.(string)
		ext.rebalanceByTopic(topic)
	}

}
func (ext *RebalanceImplExt)RemoveProcessQueue(mq *message.MessageQueue) {
	prev, _ := ext.ProcessQueueTable.Remove(mq)
	if prev != nil {
		pq := prev.(*consumer.ProcessQueue)
		pq.Dropped = true
		ext.RebalanceImpl.RemoveUnnecessaryMessageQueue(mq, pq)
	}

}

func (ext *RebalanceImplExt)rebalanceByTopic(topic string) {
	switch ext.MessageModel {
	case heartbeat.BROADCASTING://todo 广播消费后续添加
	case heartbeat.CLUSTERING:
		mqSet, _ := ext.TopicSubscribeInfoTable.Get(topic)
		cidAll := ext.MQClientFactory.findConsumerIdList(topic, ext.ConsumerGroup)
		if mqSet != nil&& len(mqSet.(set.Set).ToSlice()) > 0 && len(cidAll) > 0 {
			mqAll := []*message.MessageQueue{}
			for val := range mqSet.(set.Set).Iterator().C {
				mqAll = append(mqAll, val.(*message.MessageQueue))
			}
			var mqs message.MessageQueues = mqAll
			sort.Sort(mqs)
			sort.Strings(cidAll)
			strategy := ext.AllocateMessageQueueStrategy
			allocateResult := strategy.Allocate(ext.ConsumerGroup, ext.MQClientFactory.ClientId, mqAll, cidAll)
			allocateResultSet := set.NewSet()
			for _, mq := range allocateResult {
				allocateResultSet.Add(mq)
			}
			changed := ext.updateProcessQueueTableInRebalance(topic, allocateResultSet)
			if changed {
				logger.Infof(
					"rebalanced allocate source. allocateMessageQueueStrategyName, group, topic, mqAllSize, cidAllSize, mqAll, cidAll")
				logger.Infof(
					"rebalanced result changed. allocateMessageQueueStrategyName, group, topic, ConsumerId, rebalanceSize, rebalanceMqSet")
				mqSet := set.NewSet()
				for _, mq := range mqAll {
					mqSet.Add(mq)
				}
				ext.RebalanceImpl.MessageQueueChanged(topic, mqSet, allocateResultSet)
			}
		}
	}

}

func (ext *RebalanceImplExt)updateProcessQueueTableInRebalance(topic string, mqSet set.Set) bool {
	changed := false
	for ite := ext.ProcessQueueTable.Iterator(); ite.HasNext(); {
		msgQ, pQ, _ := ite.Next()
		mq := msgQ.(*message.MessageQueue)
		pq := pQ.(*consumer.ProcessQueue)
		if strings.EqualFold(mq.Topic, topic) {
			containsFlag := false
			for mqs := range mqSet.Iterator().C {
				ms := mqs.(*message.MessageQueue)
				if strings.EqualFold(ms.Topic, mq.Topic)&&strings.EqualFold(ms.BrokerName, mq.BrokerName) && ms.QueueId == mq.QueueId {
					containsFlag = true
					break
				}
			}
			//todo contains不支持指针
			//if !mqSet.Contains(mq) {
			if !containsFlag {
				pq.Dropped = true
				if ext.RebalanceImpl.RemoveUnnecessaryMessageQueue(mq, pq) {
					ite.Remove()
					changed = true
					logger.Infof("doRebalance, remove unnecessary")
				}
			} else if pq.IsPullExpired() {
				switch ext.RebalanceImpl.ConsumeType() {
				case heartbeat.CONSUME_ACTIVELY:
				case heartbeat.CONSUME_PASSIVELY:
					pq.Dropped = true
					if ext.RebalanceImpl.RemoveUnnecessaryMessageQueue(mq, pq) {
						ite.Remove()
						changed = true
						logger.Error("[BUG]doRebalance, remove unnecessary mq, because pull is pause, so try to fixed it")
					}
				default:
					break

				}
			}
		}
	}
	pullRequestList := []*consumer.PullRequest{}
	for mq := range mqSet.Iterator().C {
		pq, _ := ext.ProcessQueueTable.Get(mq)
		if pq == nil {
			pullRequest := &consumer.PullRequest{
				ConsumerGroup:ext.ConsumerGroup,
				MessageQueue:mq.(*message.MessageQueue),
				ProcessQueue:consumer.NewProcessQueue(),
			}
			nextOffset := ext.RebalanceImpl.ComputePullFromWhere(mq.(*message.MessageQueue))
			if nextOffset >= 0 {
				pullRequest.NextOffset = nextOffset
				pullRequestList = append(pullRequestList, pullRequest)
				changed = true
				ext.ProcessQueueTable.Put(mq, pullRequest.ProcessQueue)
				logger.Infof("doRebalance, add a new mq")
			} else {
				logger.Warnf("doRebalance, add new mq failed")
			}
		}
	}
	ext.RebalanceImpl.DispatchPullRequest(pullRequestList)
	return changed
}

func (ext *RebalanceImplExt)destroy() {
	for ite := ext.ProcessQueueTable.Iterator(); ite.HasNext(); {
		_, pq, _ := ite.Next()
		pq.(*consumer.ProcessQueue).Dropped = true
	}
	ext.ProcessQueueTable.Clear()
}
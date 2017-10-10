package process

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/rebalance"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	set "github.com/deckarep/golang-set"
	"sort"
	"strings"
)

// RebalanceImpl: rebalance接口
// Author: yintongqiang
// Since:  2017/8/11

type RebalanceImpl interface {
	// 消费类型(主动或被动)
	ConsumeType() heartbeat.ConsumeType
	// 队列变更主要用于Schedule service for pull consumer todo 暂未实现
	MessageQueueChanged(topic string, mqAll set.Set, mqDivided set.Set)
	// 删除不必要的,非法的queue
	RemoveUnnecessaryMessageQueue(mq *message.MessageQueue, pq *consumer.ProcessQueue) bool
	// 分发拉取消息请求(拉取消息真正入口)
	DispatchPullRequest(pullRequestList []*consumer.PullRequest)
	// 计算队列拉取位置
	ComputePullFromWhere(mq *message.MessageQueue) int64
}

// RebalanceImplExt: 接口基础属性
// Author: yintongqiang
// Since:  2017/8/11

type RebalanceImplExt struct {
	RebalanceImpl                RebalanceImpl
	ProcessQueueTable            *sync.Map //*MessageQueue, *ProcessQueue
	TopicSubscribeInfoTable      *sync.Map //topic  Set<*MessageQueue>
	SubscriptionInner            *sync.Map //topic, *SubscriptionData
	ConsumerGroup                string
	MessageModel                 heartbeat.MessageModel
	AllocateMessageQueueStrategy rebalance.AllocateMessageQueueStrategy
	MQClientFactory              *MQClientInstance
}

func NewRebalanceImplExt(rebalanceImpl RebalanceImpl) *RebalanceImplExt {
	return &RebalanceImplExt{
		RebalanceImpl:           rebalanceImpl,
		ProcessQueueTable:       sync.NewMap(),
		TopicSubscribeInfoTable: sync.NewMap(),
		SubscriptionInner:       sync.NewMap()}
}

// 遍历topic执行rebalance
func (ext *RebalanceImplExt) doRebalance() {
	for ite := ext.SubscriptionInner.Iterator(); ite.HasNext(); {
		k, _, _ := ite.Next()
		topic := k.(string)
		ext.rebalanceByTopic(topic)
	}

}

// 删除内存中非法的消费处理队列
func (ext *RebalanceImplExt) RemoveProcessQueue(mq *message.MessageQueue) {
	prev, _ := ext.ProcessQueueTable.Remove(mq)
	if prev != nil {
		pq := prev.(*consumer.ProcessQueue)
		pq.Dropped = true
		ext.RebalanceImpl.RemoveUnnecessaryMessageQueue(mq, pq)
	}

}

// 负载topic
func (ext *RebalanceImplExt) rebalanceByTopic(topic string) {
	switch ext.MessageModel {
	case heartbeat.BROADCASTING:
		mqSet, _ := ext.TopicSubscribeInfoTable.Get(topic)
		if mqSet != nil {
			changed := ext.updateProcessQueueTableInRebalance(topic, mqSet.(set.Set))
			if changed {
				ext.RebalanceImpl.MessageQueueChanged(topic, mqSet.(set.Set), mqSet.(set.Set))
				logger.Infof("messageQueueChanged %v %v", ext.ConsumerGroup, topic)
			}
		} else {
			logger.Warnf("doRebalance, %v, but the topic[%v] not exist.", ext.ConsumerGroup, topic)
		}
	case heartbeat.CLUSTERING:
		mqSet, _ := ext.TopicSubscribeInfoTable.Get(topic)
		cidAll := ext.MQClientFactory.findConsumerIdList(topic, ext.ConsumerGroup)
		if mqSet != nil && len(mqSet.(set.Set).ToSlice()) > 0 && len(cidAll) > 0 {
			mqAll := []*message.MessageQueue{}
			for val := range mqSet.(set.Set).Iterator().C {
				mqAll = append(mqAll, val.(*message.MessageQueue))
			}
			var mqs message.MessageQueues = mqAll
			// 结构体排序
			sort.Sort(mqs)
			// 字符串排序
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

func (ext *RebalanceImplExt) updateProcessQueueTableInRebalance(topic string, mqSet set.Set) bool {
	defer func() {
		if e := recover(); e != nil {
			panic(e)
		}
	}()
	changed := false
	for ite := ext.ProcessQueueTable.Iterator(); ite.HasNext(); {
		msgQ, pQ, _ := ite.Next()
		mq := msgQ.(*message.MessageQueue)
		pq := pQ.(*consumer.ProcessQueue)
		if strings.EqualFold(mq.Topic, topic) {
			containsFlag := false
			for mqs := range mqSet.Iterator().C {
				ms := mqs.(*message.MessageQueue)
				if ms.Equal(*mq) {
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
						logger.Errorf("[BUG]doRebalance, remove unnecessary mq=%v, because pull is pause, so try to fixed it,pq=%v", mq.ToString(), pq.ToString())
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
				ConsumerGroup: ext.ConsumerGroup,
				MessageQueue:  mq.(*message.MessageQueue),
				ProcessQueue:  consumer.NewProcessQueue(),
			}
			nextOffset := ext.RebalanceImpl.ComputePullFromWhere(mq.(*message.MessageQueue))
			if nextOffset >= 0 {
				pullRequest.NextOffset = nextOffset
				pullRequestList = append(pullRequestList, pullRequest)
				changed = true
				ext.ProcessQueueTable.Put(mq, pullRequest.ProcessQueue)
				logger.Infof("doRebalance, add a new mq success")
			} else {
				logger.Warnf("doRebalance, add new mq failed")
			}
		}
	}
	// 消费入口
	ext.RebalanceImpl.DispatchPullRequest(pullRequestList)
	return changed
}

// 清除消费处理队列(用于shutdown)
func (ext *RebalanceImplExt) destroy() {
	for ite := ext.ProcessQueueTable.Iterator(); ite.HasNext(); {
		_, pq, _ := ite.Next()
		pq.(*consumer.ProcessQueue).Dropped = true
	}
	ext.ProcessQueueTable.Clear()
}

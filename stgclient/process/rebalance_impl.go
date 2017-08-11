package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	set "github.com/deckarep/golang-set"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/rebalance"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
)
// RebalanceImpl: rebalance接口
// Author: yintongqiang
// Since:  2017/8/11


type RebalanceImpl interface {
	ConsumeType() heartbeat.ConsumeType
	MessageQueueChanged(topic string, mqAll set.Set, mqDivided set.Set)
	RemoveUnnecessaryMessageQueue(mq message.MessageQueue, pq consumer.ProcessQueue) bool
	DispatchPullRequest(pullRequestList []consumer.PullRequest)
	ComputePullFromWhere(mq message.MessageQueue) int64
}
// RebalanceImplExt: 接口基础属性
// Author: yintongqiang
// Since:  2017/8/11

type RebalanceImplExt  struct {
	ProcessQueueTable            *sync.Map
	TopicSubscribeInfoTable      *sync.Map
	SubscriptionInner            *sync.Map
	ConsumerGroup                string
	MessageModel                 heartbeat.MessageModel
	AllocateMessageQueueStrategy rebalance.AllocateMessageQueueStrategy
	MQClientFactory              *MQClientInstance
}

func NewRebalanceImplExt() RebalanceImplExt {
	return RebalanceImplExt{
		ProcessQueueTable:sync.NewMap(),
		TopicSubscribeInfoTable:sync.NewMap(),
		SubscriptionInner:sync.NewMap()}
}

func (ext RebalanceImplExt)doRebalance() {
	for ite := ext.SubscriptionInner.Iterator(); ite.HasNext(); {
		k, _, _ := ite.Next()
		topic := k.(string)
		ext.rebalanceByTopic(topic)
	}

}

func (ext RebalanceImplExt)rebalanceByTopic(topic string) {
	switch ext.MessageModel {
	case heartbeat.BROADCASTING://todo 广播消费后续添加
	case heartbeat.CLUSTERING:
		mqSet, _ := ext.TopicSubscribeInfoTable.Get(topic)

		cidAll := ext.MQClientFactory.findConsumerIdList(topic, ext.ConsumerGroup)
		if mqSet != nil&& len(mqSet.(set.Set).ToSlice()) > 0 && len(cidAll) > 0 {
			mqAll:=[]message.MessageQueue{}
			for val := range mqSet.(set.Set).Iterator().C {
				mqAll=append(mqAll,val.(message.MessageQueue))
			}
			strategy:=ext.AllocateMessageQueueStrategy
			strategy.Allocate(ext.ConsumerGroup,ext.MQClientFactory.ClientId,mqAll,cidAll)
		}
	}

}

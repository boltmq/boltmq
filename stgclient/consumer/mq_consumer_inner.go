package consumer
// MQConsumerInner: 消费内部使用接口
// Author: yintongqiang
// Since:  2017/8/9
import (
	set "github.com/deckarep/golang-set"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
)

type MQConsumerInner interface {
	// Set<SubscriptionData>
	Subscriptions() set.Set
	// Set<MessageQueue>
	UpdateTopicSubscribeInfo(topic string, info set.Set)
	GroupName() string
	MessageModel() heartbeat.MessageModel
	ConsumeType() heartbeat.ConsumeType
	ConsumeFromWhere() heartbeat.ConsumeFromWhere
	IsUnitMode() bool
	IsSubscribeTopicNeedUpdate(topic string) bool
	// 持久化offset
	PersistConsumerOffset()
    // 负载
	DoRebalance()
}

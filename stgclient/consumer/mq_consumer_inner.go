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
	// 组名称
	GroupName() string
	// 消息类型
	MessageModel() heartbeat.MessageModel
	// 消费类型
	ConsumeType() heartbeat.ConsumeType
	// 消费位置
	ConsumeFromWhere() heartbeat.ConsumeFromWhere
	IsUnitMode() bool
	// 是否需要更新
	IsSubscribeTopicNeedUpdate(topic string) bool
	// 持久化offset
	PersistConsumerOffset()
    // 负载
	DoRebalance()
}

package subscription

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// SubscriptionGroupConfig 订阅关系配置
// Author gaoyanlei
// Since 2017/8/9
type SubscriptionGroupConfig struct {
	// 订阅组名
	GroupName string `json:"groupName"`

	// 消费功能是否开启
	ConsumeEnable bool `json:"consumeEnable"`

	// 是否允许从队列最小位置开始消费，线上默认会设置为false
	ConsumeFromMinEnable bool `json:"consumeFromMinEnable"`

	// 是否允许广播方式消费
	ConsumeBroadcastEnable bool `json:"consumeBroadcastEnable"`

	// 消费失败的消息放到一个重试队列，每个订阅组配置几个重试队列
	RetryQueueNums int32 `json:"retryQueueNums"`

	// 重试消费最大次数，超过则投递到死信队列，不再投递，并报警
	RetryMaxTimes int32 `json:"retryMaxTimes"`

	// 从哪个Broker开始消费
	BrokerId int64 `json:"brokerId"`

	// 发现消息堆积后，将Consumer的消费请求重定向到另外一台Slave机器
	WhichBrokerWhenConsumeSlowly int64 `json:"whichBrokerWhenConsumeSlowly"`

	*protocol.RemotingSerializable
}

// NewSubscriptionGroupConfig 初始化SubscriptionGroupConfig
// Author gaoyanlei
// Since 2017/8/9
func NewSubscriptionGroupConfig() *SubscriptionGroupConfig {
	return &SubscriptionGroupConfig{
		ConsumeEnable:          true,
		ConsumeFromMinEnable:   true,
		ConsumeBroadcastEnable: true,
		RetryQueueNums:         1,
		RetryMaxTimes:          16,
		BrokerId:               stgcommon.MASTER_ID,
	}

}

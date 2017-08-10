package subscription

import "git.oschina.net/cloudzone/smartgo/stgcommon"

// SubscriptionGroupConfig 订阅关系配置
// @author gaoyanlei
// @since 2017/8/9
type SubscriptionGroupConfig struct {
	// 订阅组名
	GroupName string
	// 消费功能是否开启
	ConsumeEnable bool
	// 是否允许从队列最小位置开始消费，线上默认会设置为false
	ConsumeFromMinEnable bool
	// 是否允许广播方式消费
	ConsumeBroadcastEnable bool
	// 消费失败的消息放到一个重试队列，每个订阅组配置几个重试队列
	RetryQueueNums int
	// 重试消费最大次数，超过则投递到死信队列，不再投递，并报警
	RetryMaxTimes int
	// 从哪个Broker开始消费
	BrokerId int64
	// 发现消息堆积后，将Consumer的消费请求重定向到另外一台Slave机器
	WhichBrokerWhenConsumeSlowly int64
}

// NewSubscriptionGroupConfig 初始化SubscriptionGroupConfig
// @author gaoyanlei
// @since 2017/8/9
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

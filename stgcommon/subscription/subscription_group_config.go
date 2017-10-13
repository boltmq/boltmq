package subscription

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
)

// SubscriptionGroupConfig 订阅关系配置
// Author gaoyanlei
// Since 2017/8/9
type SubscriptionGroupConfig struct {
	GroupName                    string `json:"groupName"`                    // 订阅组名
	ConsumeEnable                bool   `json:"consumeEnable"`                // 消费功能是否开启
	ConsumeFromMinEnable         bool   `json:"consumeFromMinEnable"`         // 是否允许从队列最小位置开始消费(线上默认会设置为false)
	ConsumeBroadcastEnable       bool   `json:"consumeBroadcastEnable"`       // 是否允许广播方式消费
	RetryQueueNums               int32  `json:"retryQueueNums"`               // 每个订阅组配置几个重试队列(消费失败的消息放到一个重试队列)
	RetryMaxTimes                int32  `json:"retryMaxTimes"`                // 重试消费最大次数(超过最大次数，则投递到死信队列并且不再投递，并报警)
	BrokerId                     int64  `json:"brokerId"`                     // 从哪个Broker开始消费
	WhichBrokerWhenConsumeSlowly int64  `json:"whichBrokerWhenConsumeSlowly"` // 发现消息堆积后，将Consumer的消费请求重定向到另外一台Slave机器
}

// NewSubscriptionGroupConfig 初始化SubscriptionGroupConfig
// Author gaoyanlei
// Since 2017/8/9
func NewSubscriptionGroupConfig() *SubscriptionGroupConfig {
	return &SubscriptionGroupConfig{
		ConsumeEnable:          true,
		ConsumeFromMinEnable:   true,
		ConsumeBroadcastEnable: true,
		RetryQueueNums:         1,  // 每个订阅组配置重试队列的个数
		RetryMaxTimes:          16, // 重试消费最大次数
		BrokerId:               stgcommon.MASTER_ID,
	}
}

// ToString 打印SubscriptionGroupConfig结构体数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/13
func (self *SubscriptionGroupConfig) ToString() string {
	if self == nil {
		return "SubscriptionGroupConfig is nil"
	}

	format := "SubscriptionGroupConfig {groupName=%s, consumeEnable=%t, consumeFromMinEnable=%t, consumeBroadcastEnable=%t"
	format += "retryQueueNums=%d, retryMaxTimes=%d, brokerId=%d, whichBrokerWhenConsumeSlowly=%d}"
	info := fmt.Sprintf(format, self.GroupName, self.ConsumeEnable, self.ConsumeFromMinEnable, self.ConsumeBroadcastEnable,
		self.RetryQueueNums, self.RetryMaxTimes, self.BrokerId, self.WhichBrokerWhenConsumeSlowly)
	return info
}

package process

import set "github.com/deckarep/golang-set"
// MQProducerInner client内部使用发送接口
// Author: yintongqiang
// Since:  2017/8/8

type MQProducerInner interface {
	// 获取生产者topic信息列表
	GetPublishTopicList() set.Set
	// topic信息是否需要更新
	IsPublishTopicNeedUpdate(topic string) bool
    // 更新topic信息
	UpdateTopicPublishInfo(topic string, info *TopicPublishInfo)
}

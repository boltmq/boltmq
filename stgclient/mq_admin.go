package stgclient

// MQAdmin: MQ管理
// Author: yintongqiang
// Since:  2017/8/8

type MQAdmin interface {
	// key 消息队列已存在的topic
	// newTopic 需新建的topic
	// queueNum 读写队列的数量
	CreateTopic(key, newTopic string, queueNum int) error
}

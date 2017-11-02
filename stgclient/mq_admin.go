package stgclient

import (
	//"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)

// MQAdmin MQ管理
// Author yintongqiang
// Since  2017/8/8
type MQAdmin interface {
	// 创建Topic
	// key 消息队列已存在的topic
	// newTopic 需新建的topic
	// queueNum 读写队列的数量
	CreateTopic(key, newTopic string, queueNum int) error

	// 创建Topic
	// key 消息队列已存在的topic
	// newTopic 需新建的topic
	// queueNum 读写队列的数量
	CreateCustomTopic(key, newTopic string, queueNum, topicSysFlag int) error

	//// Query messages
	//// topic  message topic
	//// key    message key index word
	//// maxNum max message number
	//// begin  from when
	//// end    to when
	//QueryMessage(topic, key string, maxNum int, begin, end int64) (*QueryResult, error)
	//
	//// Query message according tto message id
	//// msgId the message id
	//ViewMessage(msgId string) (*message.MessageExt, error)
	//
	//// Gets the earliest stored message time
	//// mq Instance of MessageQueue
	//// return the time in microseconds
	//EarliestMsgStoreTime(mq *message.MessageQueue) (int64, error)
	//
	//// Gets the message queue offset according to some time in milliseconds<br> ,
	//// be cautious to call because of more IO overhead
	//// mq Instance of MessageQueue
	//// timestamp from when in milliseconds.
	//// return offset
	//SearchOffset(mq message.MessageQueue, timestamp int64) (int64, error)
	//
	//// Gets the max offset
	//// mq Instance of MessageQueue
	//// return the max offset
	//MaxOffset(mq *message.MessageQueue) (int64, error)
	//
	//// Gets the minimum offset
	//// mq Instance of MessageQueue
	//// return the max offset
	//MinOffset(mq *message.MessageQueue) (int64, error)
}

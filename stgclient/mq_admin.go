package stgclient

// MQAdmin MQ管理
//
// 注意:
// (1)MQAdmin接口不能引入"git.oschina.net/cloudzone/smartgo/stgcommon/message"包，否则产生循环引用
// (2)把属于MQAdmin接口的部分方法移入MQAdminExtInner接口，用来解决包循环引用的问题
// (3)移动的方法包括 searchOffset()、maxOffset()、minOffset()、earliestMsgStoreTime()、viewMessage()、queryMessage()
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/2
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
}

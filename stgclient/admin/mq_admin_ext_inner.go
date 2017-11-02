package admin

import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/admin"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgcommon/subscription"
	set "github.com/deckarep/golang-set"
)

// MQAdminExtInner 运维接口
//
// (1)MQ管理类接口，涉及所有与MQ管理相关的对外接口
// (2)包括Topic创建、订阅组创建、配置修改等
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
type MQAdminExtInner interface {
	stgclient.MQAdmin

	// 启动Admin
	Start() error

	// 关闭Admin
	Shutdown() error

	// 更新Broker配置
	UpdateBrokerConfig(brokerAddr string, properties map[string]interface{}) error

	// 向指定Broker创建或者更新Topic配置
	CreateAndUpdateTopicConfig(addr string, config *stgcommon.TopicConfig) error

	// 向指定Broker创建或者更新订阅组配置
	CreateAndUpdateSubscriptionGroupConfig(addr string, config *subscription.SubscriptionGroupConfig) error

	// 查询指定Broker的订阅组配置
	ExamineSubscriptionGroupConfig(addr, group string) (*subscription.SubscriptionGroupConfig, error)

	// 查询指定Broker的Topic配置
	ExamineTopicConfig(addr, topic string) (*subscription.SubscriptionGroupConfig, error)

	// 查询Topic Offset信息
	ExamineTopicStats(topic string) (*admin.TopicStatsTable, error)

	// 从Name Server获取所有Topic列表
	FetchAllTopicList() (*body.TopicList, error)

	// 获取Broker运行时数据
	FetchBrokerRuntimeStats(brokerAddr string) (*body.KVTable, error)

	// 查询消费进度
	ExamineConsumeStats(consumerGroup string) (*admin.ConsumeStats, error)

	// 基于Topic查询消费进度
	ExamineConsumeStatsByTopic(consumerGroup, topic string) (*admin.ConsumeStats, error)

	// 查看集群信息
	ExamineBrokerClusterInfo() (*body.ClusterInfo, error)

	// 查看Topic路由信息
	ExamineTopicRouteInfo(topic string) (*route.TopicRouteData, error)

	// 查看Consumer网络连接、订阅关系
	ExamineConsumerConnectionInfo(consumerGroup string) (*header.ConsumerConnection, error)

	// 查看Producer网络连接
	ExamineProducerConnectionInfo(producerGroup, topic string) (*body.ProducerConnection, error)

	// 获取Name Server地址列表
	GetNameServerAddressList() ([]string, error)

	// 清除某个Broker的写权限，针对所有Name Server
	// return 返回清除了多少个topic
	WipeWritePermOfBroker(namesrvAddr, brokerName string) (int, error)

	// 向Name Server增加一个配置项
	PutKVConfig(namespace, key, value string) error

	// 从Name Server获取一个配置项
	GetKVConfig(namespace, key string) (string, error)

	// 在 namespace 上添加或者更新 KV 配置
	CreateAndUpdateKvConfig(namespace, key, value string) error

	// 删除 namespace 上的 KV 配置
	DeleteKvConfig(namespace, key string) error

	// 获取指定Namespace下的所有kv
	GetKVListByNamespace(namespace string) (*body.KVTable, error)

	// 删除 broker 上的 topic 信息
	DeleteTopicInBroker(brokerAddrs set.Set, topic string) error

	// 删除 namesrv维护的topic信息
	DeleteTopicInNameServer(namesrvs set.Set, topic string) error

	// 删除 broker 上的 subscription group 信息
	DeleteSubscriptionGroup(brokerAddr, groupName string) error

	// 通过 server ip 获取 project 信息
	GetProjectGroupByIp(ip string) (string, error)

	// 通过 project 获取所有的 server ip 信息
	GetIpsByProjectGroup(projectGroup string) (string, error)

	// 删除 project group 对应的所有 server ip
	DeleteIpsByProjectGroup(key string) error

	// 按照时间回溯消费进度(客户端需要重启)
	ResetOffsetByTimestampOld(consumerGroup, topic string, timestamp int64, force bool) ([]*admin.RollbackStats, error)

	// 按照时间回溯消费进度(客户端不需要重启)
	ResetOffsetByTimestamp(topic, group string, timestamp int64, force bool) (map[message.MessageQueue]int64, error)

	// 重置消费进度，无论Consumer是否在线，都可以执行。不保证最终结果是否成功，需要调用方通过消费进度查询来再次确认
	ResetOffsetNew(consumerGroup, topic string, timestamp int64) error

	// 通过客户端查看消费者的消费情况
	GetConsumeStatus(topic, consumerGroupId, clientAddr string) (map[string]map[message.MessageQueue]int64, error)

	// 创建或更新顺序消息的分区配置
	CreateOrUpdateOrderConf(key, value string, isCluster bool) error

	// 根据Topic查询被哪些订阅组消费
	QueryTopicConsumeByWho(topic string) (*body.GroupList, error)

	// 根据 topic 和 group 获取消息的时间跨度
	// retutn set<QueueTimeSpan>
	QueryConsumeTimeSpan(topic, group string) (set.Set, error)

	// 触发清理失效的消费队列
	// cluster 如果参数cluster为空，则表示所有集群
	// return 清理是否成功
	CleanExpiredConsumerQueue(cluster string) (bool, error)

	// 触发指定的broker清理失效的消费队列
	// return 清理是否成功
	CleanExpiredConsumerQueueByAddr(addr string)

	// 查询Consumer内存数据结构
	GetConsumerRunningInfo(consumerGroupId, clientId string, jstack bool) (*body.ConsumerRunningInfo, error)

	// 向指定Consumer发送某条消息
	ConsumeMessageDirectly(consumerGroup, clientId, msgId string) (*body.ConsumeMessageDirectlyResult, error)

	//查询消息被谁消费了
	MessageTrackDetail(msg *message.MessageExt) ([]*MessageTrack, error)

	// 克隆某一个组的消费进度到新的组
	CloneGroupOffset(srcGroup, destGroup, topic string, isOffline bool) error

	// 服务器统计数据输出
	ViewBrokerStatsData(brokerAddr, statsName, statsKey string) (*body.BrokerStatsData, error)

	// 根据msgId查询消息消费结果
	ViewMessage(msgId string) (*message.MessageExt, error)

	// 搜索消息
	// topic  topic名称
	// key    消息key关键字[业务系统基于此字段唯一标识消息]
	// maxNum 最大搜索条数
	// begin  开始查询消息的时间戳
	// end    结束查询消息的时间戳
	QueryMessage(topic, key string, maxNum int, begin, end int64) (*stgclient.QueryResult, error)

	// 查询较早的存储消息
	EarliestMsgStoreTime(mq *message.MessageQueue) (int64, error)

	// 根据时间戳搜索MessageQueue偏移量(注意:可能会出现大量IO开销)
	SearchOffset(mq message.MessageQueue, timestamp int64) (int64, error)

	// 查询MessageQueue最大偏移量
	MaxOffset(mq *message.MessageQueue) (int64, error)

	// 查询MessageQueue最小偏移量
	MinOffset(mq *message.MessageQueue) (int64, error)
}

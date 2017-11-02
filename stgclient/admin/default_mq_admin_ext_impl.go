package admin

import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/admin"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgcommon/subscription"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	set "github.com/deckarep/golang-set"
)

// DefaultMQAdminExtImpl 所有运维接口都在这里实现
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/2
type DefaultMQAdminExtImpl struct {
	ServiceState     stgcommon.ServiceState
	MqClientInstance process.MQClientInstance
	RpcHook          remoting.RPCHook
	ClientConfig     *stgclient.ClientConfig
	MQAdminExtInner
}

func NewDefaultMQAdminExtImpl() *DefaultMQAdminExtImpl {
	defaultMQAdminExtImpl := &DefaultMQAdminExtImpl{}
	defaultMQAdminExtImpl.ServiceState = stgcommon.ServiceState(stgcommon.CREATE_JUST)
	return defaultMQAdminExtImpl
}

func NewCustomMQAdminExtImpl(rpcHook remoting.RPCHook) *DefaultMQAdminExtImpl {
	defaultMQAdminExtImpl := NewDefaultMQAdminExtImpl()
	defaultMQAdminExtImpl.RpcHook = rpcHook
	return defaultMQAdminExtImpl
}

// 启动Admin
func (impl *DefaultMQAdminExtImpl) Start() error {
	return nil
}

// 关闭Admin
func (impl *DefaultMQAdminExtImpl) Shutdown() error {
	return nil

}

// 更新Broker配置
func (impl *DefaultMQAdminExtImpl) UpdateBrokerConfig(brokerAddr string, properties map[string]interface{}) error {
	return nil
}

// 向指定Broker创建或者更新Topic配置
func (impl *DefaultMQAdminExtImpl) CreateAndUpdateTopicConfig(addr string, config *stgcommon.TopicConfig) error {
	return nil
}

// 向指定Broker创建或者更新订阅组配置
func (impl *DefaultMQAdminExtImpl) CreateAndUpdateSubscriptionGroupConfig(addr string, config *subscription.SubscriptionGroupConfig) error {
	return nil
}

// 查询指定Broker的订阅组配置
func (impl *DefaultMQAdminExtImpl) ExamineSubscriptionGroupConfig(addr, group string) (*subscription.SubscriptionGroupConfig, error) {
	return nil, nil
}

// 查询指定Broker的Topic配置
func (impl *DefaultMQAdminExtImpl) ExamineTopicConfig(addr, topic string) (*subscription.SubscriptionGroupConfig, error) {
	return nil, nil
}

// 查询Topic Offset信息
func (impl *DefaultMQAdminExtImpl) ExamineTopicStats(topic string) (*admin.TopicStatsTable, error) {
	return nil, nil
}

// 从Name Server获取所有Topic列表
func (impl *DefaultMQAdminExtImpl) FetchAllTopicList() (*body.TopicList, error) {
	return nil, nil
}

// 获取Broker运行时数据
func (impl *DefaultMQAdminExtImpl) FetchBrokerRuntimeStats(brokerAddr string) (*body.KVTable, error) {
	return nil, nil
}

// 查询消费进度
func (impl *DefaultMQAdminExtImpl) ExamineConsumeStats(consumerGroup string) (*admin.ConsumeStats, error) {
	return nil, nil
}

// 基于Topic查询消费进度
func (impl *DefaultMQAdminExtImpl) ExamineConsumeStatsByTopic(consumerGroup, topic string) (*admin.ConsumeStats, error) {
	return nil, nil
}

// 查看集群信息
func (impl *DefaultMQAdminExtImpl) ExamineBrokerClusterInfo() (*body.ClusterInfo, error) {
	return nil, nil

}

// 查看Topic路由信息
func (impl *DefaultMQAdminExtImpl) ExamineTopicRouteInfo(topic string) (*route.TopicRouteData, error) {
	return nil, nil
}

// 查看Consumer网络连接、订阅关系
func (impl *DefaultMQAdminExtImpl) ExamineConsumerConnectionInfo(consumerGroup string) (*header.ConsumerConnection, error) {
	return nil, nil
}

// 查看Producer网络连接
func (impl *DefaultMQAdminExtImpl) ExamineProducerConnectionInfo(producerGroup, topic string) (*body.ProducerConnection, error) {
	return nil, nil
}

// 获取Name Server地址列表
func (impl *DefaultMQAdminExtImpl) GetNameServerAddressList() ([]string, error) {
	return []string{}, nil
}

// 清除某个Broker的写权限，针对所有Name Server
// return 返回清除了多少个topic
func (impl *DefaultMQAdminExtImpl) WipeWritePermOfBroker(namesrvAddr, brokerName string) (int, error) {
	return 0, nil
}

// 向Name Server增加一个配置项
func (impl *DefaultMQAdminExtImpl) PutKVConfig(namespace, key, value string) error {
	return nil
}

// 从Name Server获取一个配置项
func (impl *DefaultMQAdminExtImpl) GetKVConfig(namespace, key string) (string, error) {
	return "", nil
}

// 在 namespace 上添加或者更新 KV 配置
func (impl *DefaultMQAdminExtImpl) CreateAndUpdateKvConfig(namespace, key, value string) error {
	return nil
}

// 删除 namespace 上的 KV 配置
func (impl *DefaultMQAdminExtImpl) DeleteKvConfig(namespace, key string) error {
	return nil
}

// 获取指定Namespace下的所有kv
func (impl *DefaultMQAdminExtImpl) GetKVListByNamespace(namespace string) (*body.KVTable, error) {
	return nil, nil
}

// 删除 broker 上的 topic 信息
func (impl *DefaultMQAdminExtImpl) DeleteTopicInBroker(brokerAddrs set.Set, topic string) error {
	return nil
}

// 删除 namesrv维护的topic信息
func (impl *DefaultMQAdminExtImpl) DeleteTopicInNameServer(namesrvs set.Set, topic string) error {
	return nil
}

// 删除 broker 上的 subscription group 信息
func (impl *DefaultMQAdminExtImpl) DeleteSubscriptionGroup(brokerAddr, groupName string) error {
	return nil
}

// 通过 server ip 获取 project 信息
func (impl *DefaultMQAdminExtImpl) GetProjectGroupByIp(ip string) (string, error) {
	return "", nil
}

// 通过 project 获取所有的 server ip 信息
func (impl *DefaultMQAdminExtImpl) GetIpsByProjectGroup(projectGroup string) (string, error) {
	return "", nil
}

// 删除 project group 对应的所有 server ip
func (impl *DefaultMQAdminExtImpl) DeleteIpsByProjectGroup(key string) error {
	return nil
}

// 按照时间回溯消费进度(客户端需要重启)
func (impl *DefaultMQAdminExtImpl) ResetOffsetByTimestampOld(consumerGroup, topic string, timestamp int64, force bool) ([]*admin.RollbackStats, error) {
	return nil, nil
}

// 按照时间回溯消费进度(客户端不需要重启)
func (impl *DefaultMQAdminExtImpl) ResetOffsetByTimestamp(topic, group string, timestamp int64, force bool) (map[message.MessageQueue]int64, error) {
	return nil, nil
}

// 重置消费进度，无论Consumer是否在线，都可以执行。不保证最终结果是否成功，需要调用方通过消费进度查询来再次确认
func (impl *DefaultMQAdminExtImpl) ResetOffsetNew(consumerGroup, topic string, timestamp int64) error {
	return nil
}

// 通过客户端查看消费者的消费情况
func (impl *DefaultMQAdminExtImpl) GetConsumeStatus(topic, consumerGroupId, clientAddr string) (map[string]map[message.MessageQueue]int64, error) {
	return nil, nil
}

// 创建或更新顺序消息的分区配置
func (impl *DefaultMQAdminExtImpl) CreateOrUpdateOrderConf(key, value string, isCluster bool) error {
	return nil

}

// 根据Topic查询被哪些订阅组消费
func (impl *DefaultMQAdminExtImpl) QueryTopicConsumeByWho(topic string) (*body.GroupList, error) {
	return nil, nil
}

// 根据 topic 和 group 获取消息的时间跨度
// retutn set<QueueTimeSpan>
func (impl *DefaultMQAdminExtImpl) QueryConsumeTimeSpan(topic, group string) (set.Set, error) {
	return nil, nil
}

// 触发清理失效的消费队列
// cluster 如果参数cluster为空，则表示所有集群
// return 清理是否成功
func (impl *DefaultMQAdminExtImpl) CleanExpiredConsumerQueue(cluster string) (bool, error) {
	return false, nil
}

// 触发指定的broker清理失效的消费队列
// return 清理是否成功
func (impl *DefaultMQAdminExtImpl) CleanExpiredConsumerQueueByAddr(addr string) {

}

// 查询Consumer内存数据结构
func (impl *DefaultMQAdminExtImpl) GetConsumerRunningInfo(consumerGroupId, clientId string, jstack bool) (*body.ConsumerRunningInfo, error) {
	return nil, nil
}

// 向指定Consumer发送某条消息
func (impl *DefaultMQAdminExtImpl) ConsumeMessageDirectly(consumerGroup, clientId, msgId string) (*body.ConsumeMessageDirectlyResult, error) {
	return nil, nil
}

//查询消息被谁消费了
func (impl *DefaultMQAdminExtImpl) MessageTrackDetail(msg *message.MessageExt) ([]*MessageTrack, error) {
	return nil, nil
}

// 克隆某一个组的消费进度到新的组
func (impl *DefaultMQAdminExtImpl) CloneGroupOffset(srcGroup, destGroup, topic string, isOffline bool) error {
	return nil
}

// 服务器统计数据输出
func (impl *DefaultMQAdminExtImpl) ViewBrokerStatsData(brokerAddr, statsName, statsKey string) (*body.BrokerStatsData, error) {
	return nil, nil
}

// 创建Topic
// key 消息队列已存在的topic
// newTopic 需新建的topic
// queueNum 读写队列的数量
func (impl *DefaultMQAdminExtImpl) CreateTopic(key, newTopic string, queueNum int) error {
	return nil
}

// 创建Topic
// key 消息队列已存在的topic
// newTopic 需新建的topic
// queueNum 读写队列的数量
func (impl *DefaultMQAdminExtImpl) CreateCustomTopic(key, newTopic string, queueNum, topicSysFlag int) error {
	return nil
}

// 根据msgId查询消息消费结果
func (impl *DefaultMQAdminExtImpl) ViewMessage(msgId string) (*message.MessageExt, error) {
	return nil, nil
}

// 搜索消息
// topic  topic名称
// key    消息key关键字[业务系统基于此字段唯一标识消息]
// maxNum 最大搜索条数
// begin  开始查询消息的时间戳
// end    结束查询消息的时间戳
func (impl *DefaultMQAdminExtImpl) QueryMessage(topic, key string, maxNum int, begin, end int64) (*stgclient.QueryResult, error) {
	return nil, nil
}

// 查询较早的存储消息
func (impl *DefaultMQAdminExtImpl) EarliestMsgStoreTime(mq *message.MessageQueue) (int64, error) {
	return 0, nil
}

// 根据时间戳搜索MessageQueue偏移量(注意:可能会出现大量IO开销)
func (impl *DefaultMQAdminExtImpl) SearchOffset(mq message.MessageQueue, timestamp int64) (int64, error) {
	return 0, nil
}

// 查询MessageQueue最大偏移量
func (impl *DefaultMQAdminExtImpl) MaxOffset(mq *message.MessageQueue) (int64, error) {
	return 0, nil
}

// 查询MessageQueue最小偏移量
func (impl *DefaultMQAdminExtImpl) MinOffset(mq *message.MessageQueue) (int64, error) {
	return 0, nil
}

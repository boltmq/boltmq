package stgcommon

import (
	"os"
	"runtime"
)

// Broker配置
// @author gaoyanlei
// @since 2017/8/8
type BrokerConfig struct {
	// mqHome
	RocketmqHome string
	// namsrv地址
	NamesrvAddr string
	// 本机ip地址
	BrokerIP1 string
	BrokerIP2 string
	// 当前机器hostName
	BrokerName string
	// 集群名称
	BrokerClusterName string
	// mastId
	BrokerId int64
	// Broker权限
	BrokerPermission int
	// 默认topic队列数
	DefaultTopicQueueNums int
	// 自动创建Topic功能是否开启（线上建议关闭）
	AutoCreateTopicEnable bool
	// 自动创建以集群名字命名的Topic功能是否开启
	ClusterTopicEnable bool
	// 自动创建以服务器名字命名的Topic功能是否开启
	BrokerTopicEnable bool
	// 自动创建订阅组功能是否开启（线上建议关闭）
	AutoCreateSubscriptionGroup bool
	// SendMessageProcessor处理线程数
	SendMessageThreadPoolNums int
	// PullMessageProcessor处理线程数
	PullMessageThreadPoolNums int
	// AdminBrokerProcessor处理线程数
	AdminBrokerThreadPoolNums int
	// ClientManageProcessor处理线程数
	ClientManageThreadPoolNums int
	// 刷新Consumer offest定时间隔
	FlushConsumerOffsetInterval int
	// 此值cloudmq没有用到
	FlushConsumerOffsetHistoryInterval int
	// 是否拒绝接收事务消息
	RejectTransactionMessage bool
	// 是否从地址服务器寻找Name Server地址，正式发布后，默认值为false
	FetchNamesrvAddrByAddressServer bool
	// 发送消息对应的线程池阻塞队列size
	SendThreadPoolQueueCapacity int
	// 订阅消息对应的线程池阻塞队列size
	PullThreadPoolQueueCapacity int
	// 过滤服务器数量
	FilterServerNums int
	// Consumer订阅消息时，Broker是否开启长轮询
	LongPollingEnable bool
	// 如果是短轮询，服务器挂起时间
	ShortPollingTimeMills int64
	// notify consumerId changed 开关
	NotifyConsumerIdsChangedEnable bool
	// slave 是否需要纠正位点
	OffsetCheckInSlave bool
}

// NewBrokerConfig
func NewBrokerConfig() *BrokerConfig {
	var brokerConfig = new(BrokerConfig)
	brokerConfig.RocketmqHome = os.Getenv(CLOUDMQ_HOME_ENV)
	brokerConfig.NamesrvAddr = os.Getenv(NAMESRV_ADDR_ENV)

	//TODO 获取getLocalAddress
	//brokerConfig.BrokerIP1=
	//brokerConfig.BrokerIP2=

	brokerConfig.BrokerName = localHostName()
	brokerConfig.BrokerClusterName = "DefaultCluster"
	brokerConfig.BrokerId = MASTER_ID

	// TODO PermName常量
	//brokerConfig.BrokerPermission="DefaultCluster"

	brokerConfig.DefaultTopicQueueNums = 8

	// 自动创建Topic功能是否开启
	brokerConfig.AutoCreateTopicEnable = true

	brokerConfig.ClusterTopicEnable = true

	// 自动创建以服务器名字命名的Topic功能是否开启
	brokerConfig.BrokerTopicEnable = true

	// 自动创建订阅组功能是否开启（线上建议关闭）
	brokerConfig.AutoCreateSubscriptionGroup = true

	// SendMessageProcessor处理线程数
	brokerConfig.SendMessageThreadPoolNums = 16 + runtime.NumCPU()*4

	brokerConfig.PullMessageThreadPoolNums = 16 + runtime.NumCPU()*2

	brokerConfig.AdminBrokerThreadPoolNums = 16

	brokerConfig.ClientManageThreadPoolNums = 16

	brokerConfig.FlushConsumerOffsetInterval = 1000 * 5

	brokerConfig.FlushConsumerOffsetHistoryInterval = 1000 * 60
	brokerConfig.RejectTransactionMessage = false
	brokerConfig.FetchNamesrvAddrByAddressServer = false
	brokerConfig.FetchNamesrvAddrByAddressServer = false
	brokerConfig.SendThreadPoolQueueCapacity = 100000
	brokerConfig.PullThreadPoolQueueCapacity = 100000
	brokerConfig.FilterServerNums = 0
	brokerConfig.LongPollingEnable = true
	brokerConfig.ShortPollingTimeMills = 1000
	brokerConfig.NotifyConsumerIdsChangedEnable = true
	brokerConfig.OffsetCheckInSlave = true
	return brokerConfig
}

// 获取当前机器hostName
// @author gaoyanlei
// @since 2017/8/8
func localHostName() string {
	host, err := os.Hostname()
	if err != nil {
		return "DEFAULT_BROKER"
	}
	return host
}

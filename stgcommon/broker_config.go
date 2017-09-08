package stgcommon

import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"os"
	"runtime"
)

// BrokerConfig Broker配置项
// Author gaoyanlei
// Since 2017/8/8
type BrokerConfig struct {
	// mqHome
	SmartGoHome string
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
	DefaultTopicQueueNums int32
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
	FilterServerNums int32
	// Consumer订阅消息时，Broker是否开启长轮询
	LongPollingEnable bool
	// 如果是短轮询，服务器挂起时间
	ShortPollingTimeMills int
	// notify consumerId changed 开关
	NotifyConsumerIdsChangedEnable bool
	// slave 是否需要纠正位点
	OffsetCheckInSlave bool
}

// NewBrokerConfig 初始化BrokerConfig
// Author gaoyanlei
// Since 2017/8/9
func NewBrokerConfig() *BrokerConfig {
	return &BrokerConfig{
		SmartGoHome:                        os.Getenv(SMARTGO_HOME_ENV),
		NamesrvAddr:                        os.Getenv(NAMESRV_ADDR_ENV),
		BrokerIP1:                          stgclient.GetLocalAddress(),
		BrokerIP2:                          stgclient.GetLocalAddress(),
		BrokerName:                         localHostName(),
		BrokerClusterName:                  "DefaultCluster",
		BrokerId:                           MASTER_ID,
		BrokerPermission:                   6,
		DefaultTopicQueueNums:              8,
		AutoCreateTopicEnable:              true,
		ClusterTopicEnable:                 true,
		BrokerTopicEnable:                  true,
		AutoCreateSubscriptionGroup:        true,
		SendMessageThreadPoolNums:          16 + runtime.NumCPU()*4,
		PullMessageThreadPoolNums:          16 + runtime.NumCPU()*2,
		AdminBrokerThreadPoolNums:          16,
		ClientManageThreadPoolNums:         16,
		FlushConsumerOffsetInterval:        1000 * 5,
		FlushConsumerOffsetHistoryInterval: 1000 * 60,
		RejectTransactionMessage:           false,
		FetchNamesrvAddrByAddressServer:    false,
		SendThreadPoolQueueCapacity:        100000,
		PullThreadPoolQueueCapacity:        100000,
		FilterServerNums:                   0,
		LongPollingEnable:                  true,
		ShortPollingTimeMills:              1000,
		NotifyConsumerIdsChangedEnable:     true,
		OffsetCheckInSlave:                 true,
	}
}

// localHostName 获取当前机器hostName
// Author gaoyanlei
// Since 2017/8/8
func localHostName() string {
	host, err := os.Hostname()
	if err != nil {
		return "DEFAULT_BROKER"
	}
	return host
}

package stgcommon

import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"os"
	"runtime"
)

// BrokerConfig Broker配置项
// Author gaoyanlei
// Since 2017/8/8
type BrokerConfig struct {
	// mqHome
	SmartGoHome string `json:"SmartGoHome"`
	// namsrv地址
	NamesrvAddr string `json:"NamesrvAddr"`
	// 本机ip地址
	BrokerIP1 string `json:"BrokerIP1"`
	BrokerIP2 string `json:"BrokerIP2"`
	// 当前机器hostName
	BrokerName string `json:"BrokerName"`
	// 集群名称
	BrokerClusterName string `json:"BrokerClusterName"`
	// mastId
	BrokerId int64 `json:"BrokerId"`
	// Broker权限
	BrokerPermission int `json:"BrokerPermission"`
	// 默认topic队列数
	DefaultTopicQueueNums int32 `json:"DefaultTopicQueueNums"`
	// 自动创建Topic功能是否开启（线上建议关闭）
	AutoCreateTopicEnable bool `json:"AutoCreateTopicEnable"`
	// 自动创建以集群名字命名的Topic功能是否开启
	ClusterTopicEnable bool `json:"ClusterTopicEnable"`
	// 自动创建以服务器名字命名的Topic功能是否开启
	BrokerTopicEnable bool `json:"BrokerTopicEnable"`
	// 自动创建订阅组功能是否开启（线上建议关闭）
	AutoCreateSubscriptionGroup bool `json:"AutoCreateSubscriptionGroup"`
	// SendMessageProcessor处理线程数
	SendMessageThreadPoolNums int `json:"SendMessageThreadPoolNums"`
	// PullMessageProcessor处理线程数
	PullMessageThreadPoolNums int `json:"PullMessageThreadPoolNums"`
	// AdminBrokerProcessor处理线程数
	AdminBrokerThreadPoolNums int `json:"AdminBrokerThreadPoolNums"`
	// ClientManageProcessor处理线程数
	ClientManageThreadPoolNums int `json:"ClientManageThreadPoolNums"`
	// 刷新Consumer offest定时间隔
	FlushConsumerOffsetInterval int `json:"FlushConsumerOffsetInterval"`
	// 此值cloudmq没有用到
	FlushConsumerOffsetHistoryInterval int `json:"FlushConsumerOffsetHistoryInterval"`
	// 是否拒绝接收事务消息
	RejectTransactionMessage bool `json:"RejectTransactionMessage"`
	// 是否从地址服务器寻找Name Server地址，正式发布后，默认值为false
	FetchNamesrvAddrByAddressServer bool `json:"FetchNamesrvAddrByAddressServer"`
	// 发送消息对应的线程池阻塞队列size
	SendThreadPoolQueueCapacity int `json:"SendThreadPoolQueueCapacity"`
	// 订阅消息对应的线程池阻塞队列size
	PullThreadPoolQueueCapacity int `json:"PullThreadPoolQueueCapacity"`
	// 过滤服务器数量
	FilterServerNums int32 `json:"FilterServerNums"`
	// Consumer订阅消息时，Broker是否开启长轮询
	LongPollingEnable bool `json:"LongPollingEnable"`
	// 如果是短轮询，服务器挂起时间
	ShortPollingTimeMills int `json:"ShortPollingTimeMills"`
	// notify consumerId changed 开关
	NotifyConsumerIdsChangedEnable bool `json:"NotifyConsumerIdsChangedEnable"`
	// slave 是否需要纠正位点
	OffsetCheckInSlave             bool `json:"OffsetCheckInSlave"`
	*protocol.RemotingSerializable `json:"-"`
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
		RemotingSerializable:               new(protocol.RemotingSerializable),
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

package stgcommon

import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"os"
	"runtime"
)

const (
	defaultHostName          = "DEFAULT_BROKER"
	defaultBrokerClusterName = "DefaultCluster"
	defaultBrokerPermission  = 6
	defaultTopicQueueNums    = 8
)

// BrokerConfig Broker配置项
// Author gaoyanlei
// Since 2017/8/8
type BrokerConfig struct {
	SmartGoHome                        string `json:"SmartGoHome"`                        // mqHome
	NamesrvAddr                        string `json:"NamesrvAddr"`                        // namsrv地址
	BrokerIP1                          string `json:"BrokerIP1"`                          // 本机ip1地址
	BrokerIP2                          string `json:"BrokerIP2"`                          // 本机ip2地址
	BrokerName                         string `json:"BrokerName"`                         // 当前机器hostName
	BrokerClusterName                  string `json:"BrokerClusterName"`                  // 集群名称
	BrokerId                           int64  `json:"BrokerId"`                           // 默认值MasterId
	BrokerPermission                   int    `json:"BrokerPermission"`                   // Broker权限
	DefaultTopicQueueNums              int32  `json:"DefaultTopicQueueNums"`              // 默认topic队列数
	AutoCreateTopicEnable              bool   `json:"AutoCreateTopicEnable"`              // 自动创建Topic功能是否开启（生产环境建议关闭）
	ClusterTopicEnable                 bool   `json:"ClusterTopicEnable"`                 // 自动创建以集群名字命名的Topic功能是否开启
	BrokerTopicEnable                  bool   `json:"BrokerTopicEnable"`                  // 自动创建以服务器名字命名的Topic功能是否开启
	AutoCreateSubscriptionGroup        bool   `json:"AutoCreateSubscriptionGroup"`        // 自动创建订阅组功能是否开启（线上建议关闭）
	SendMessageThreadPoolNums          int    `json:"SendMessageThreadPoolNums"`          // SendMessageProcessor处理线程数
	PullMessageThreadPoolNums          int    `json:"PullMessageThreadPoolNums"`          // PullMessageProcessor处理线程数
	AdminBrokerThreadPoolNums          int    `json:"AdminBrokerThreadPoolNums"`          // AdminBrokerProcessor处理线程数
	ClientManageThreadPoolNums         int    `json:"ClientManageThreadPoolNums"`         // ClientManageProcessor处理线程数
	FlushConsumerOffsetInterval        int    `json:"FlushConsumerOffsetInterval"`        // 刷新Consumer offest定时间隔
	FlushConsumerOffsetHistoryInterval int    `json:"FlushConsumerOffsetHistoryInterval"` // 此字段目前cloudmq没有使用
	RejectTransactionMessage           bool   `json:"RejectTransactionMessage"`           // 是否拒绝接收事务消息
	FetchNamesrvAddrByAddressServer    bool   `json:"FetchNamesrvAddrByAddressServer"`    // 是否从地址服务器寻找NameServer地址，正式发布后，默认值为false
	SendThreadPoolQueueCapacity        int    `json:"SendThreadPoolQueueCapacity"`        // 发送消息对应的线程池阻塞队列size
	PullThreadPoolQueueCapacity        int    `json:"PullThreadPoolQueueCapacity"`        // 订阅消息对应的线程池阻塞队列size
	FilterServerNums                   int32  `json:"FilterServerNums"`                   // 过滤服务器数量
	LongPollingEnable                  bool   `json:"LongPollingEnable"`                  // Consumer订阅消息时，Broker是否开启长轮询
	ShortPollingTimeMills              int    `json:"ShortPollingTimeMills"`              // 如果是短轮询，服务器挂起时间
	NotifyConsumerIdsChangedEnable     bool   `json:"NotifyConsumerIdsChangedEnable"`     // notify consumerId changed 开关
	OffsetCheckInSlave                 bool   `json:"OffsetCheckInSlave"`                 // slave 是否需要纠正位点
}

// NewDefaultBrokerConfig 初始化默认BrokerConfig（默认AutoCreateTopicEnable=true）
// Author gaoyanlei
// Since 2017/8/9
func NewDefaultBrokerConfig() *BrokerConfig {
	brokerConfig := &BrokerConfig{
		SmartGoHome:                        os.Getenv(SMARTGO_HOME_ENV),
		NamesrvAddr:                        os.Getenv(NAMESRV_ADDR_ENV),
		BrokerIP1:                          stgclient.GetLocalAddress(),
		BrokerIP2:                          stgclient.GetLocalAddress(),
		BrokerName:                         localHostName(),
		BrokerClusterName:                  defaultBrokerClusterName,
		BrokerId:                           MASTER_ID,
		BrokerPermission:                   defaultBrokerPermission,
		DefaultTopicQueueNums:              defaultTopicQueueNums,
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

	return brokerConfig
}

// NewCustomBrokerConfig 初始化BrokerConfig（根据传入参数autoCreateTopicEnable来标记：是否自动创建Topic）
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/28
func NewCustomBrokerConfig(brokerName, brokerClusterName string, autoCreateTopicEnable bool) *BrokerConfig {
	brokerConfig := NewDefaultBrokerConfig()
	brokerConfig.BrokerName = brokerName
	brokerConfig.BrokerClusterName = brokerClusterName
	brokerConfig.AutoCreateTopicEnable = autoCreateTopicEnable
	return brokerConfig
}

// NewBrokerConfig 初始化BrokerConfig（默认AutoCreateTopicEnable=true）
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/28
func NewBrokerConfig(brokerName, brokerClusterName string) *BrokerConfig {
	brokerConfig := NewDefaultBrokerConfig()
	brokerConfig.BrokerName = brokerName
	brokerConfig.BrokerClusterName = brokerClusterName
	return brokerConfig
}

// localHostName 获取当前机器hostName
// Author gaoyanlei
// Since 2017/8/8
func localHostName() string {
	host, err := os.Hostname()
	if err != nil {
		return defaultHostName
	}
	return host
}

// HasReadable 校验Broker是否有读权限
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/29
func (self *BrokerConfig) HasReadable() bool {
	return constant.IsReadable(self.BrokerPermission)
}

// HasWriteable 校验Broker是否有写权限
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/29
func (self *BrokerConfig) HasWriteable() bool {
	return constant.IsWriteable(self.BrokerPermission)
}

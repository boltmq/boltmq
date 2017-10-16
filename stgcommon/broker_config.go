package stgcommon

import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"os"
	"runtime"
	"strings"
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
	SmartGoHome                        string `json:"smartgoHome"`                        // 环境变量"SMARTGO_HOME"(smartgo工程目录)
	SmartgoDataPath                    string `json:"smartgoDataPath"`                    // broker、store等模块的数据存储目录
	NamesrvAddr                        string `json:"namesrvAddr"`                        // namsrv地址
	BrokerIP1                          string `json:"brokerIP1"`                          // 本机ip1地址
	BrokerIP2                          string `json:"brokerIP2"`                          // 本机ip2地址
	BrokerName                         string `json:"brokerName"`                         // 当前机器hostName
	BrokerClusterName                  string `json:"brokerClusterName"`                  // 集群名称
	BrokerId                           int64  `json:"brokerId"`                           // 默认值MasterId
	BrokerPort                         int    `json:"brokerPort"`                         // broker对外提供服务端口
	BrokerPermission                   int    `json:"brokerPermission"`                   // Broker权限
	DefaultTopicQueueNums              int32  `json:"defaultTopicQueueNums"`              // 默认topic队列数
	AutoCreateTopicEnable              bool   `json:"autoCreateTopicEnable"`              // 自动创建Topic功能是否开启（生产环境建议关闭）
	ClusterTopicEnable                 bool   `json:"clusterTopicEnable"`                 // 自动创建以集群名字命名的Topic功能是否开启
	BrokerTopicEnable                  bool   `json:"brokerTopicEnable"`                  // 自动创建以服务器名字命名的Topic功能是否开启
	AutoCreateSubscriptionGroup        bool   `json:"autoCreateSubscriptionGroup"`        // 自动创建订阅组功能是否开启（线上建议关闭）
	SendMessageThreadPoolNums          int    `json:"sendMessageThreadPoolNums"`          // SendMessageProcessor处理线程数
	PullMessageThreadPoolNums          int    `json:"pullMessageThreadPoolNums"`          // PullMessageProcessor处理线程数
	AdminBrokerThreadPoolNums          int    `json:"adminBrokerThreadPoolNums"`          // AdminBrokerProcessor处理线程数
	ClientManageThreadPoolNums         int    `json:"clientManageThreadPoolNums"`         // ClientManageProcessor处理线程数
	FlushConsumerOffsetInterval        int    `json:"flushConsumerOffsetInterval"`        // 刷新ConsumerOffest定时间隔
	FlushConsumerOffsetHistoryInterval int    `json:"flushConsumerOffsetHistoryInterval"` // 此字段目前没有使用
	RejectTransactionMessage           bool   `json:"rejectTransactionMessage"`           // 是否拒绝接收事务消息
	FetchNamesrvAddrByAddressServer    bool   `json:"fetchNamesrvAddrByAddressServer"`    // 是否从地址服务器寻找NameServer地址，正式发布后，默认值为false
	SendThreadPoolQueueCapacity        int    `json:"sendThreadPoolQueueCapacity"`        // 发送消息对应的线程池阻塞队列size
	PullThreadPoolQueueCapacity        int    `json:"pullThreadPoolQueueCapacity"`        // 订阅消息对应的线程池阻塞队列size
	FilterServerNums                   int32  `json:"filterServerNums"`                   // 过滤服务器数量
	LongPollingEnable                  bool   `json:"longPollingEnable"`                  // Consumer订阅消息时，Broker是否开启长轮询
	ShortPollingTimeMills              int    `json:"shortPollingTimeMills"`              // 如果是短轮询，服务器挂起时间
	NotifyConsumerIdsChangedEnable     bool   `json:"notifyConsumerIdsChangedEnable"`     // notify consumerId changed 开关
	OffsetCheckInSlave                 bool   `json:"offsetCheckInSlave"`                 // slave 是否需要纠正位点
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
func NewCustomBrokerConfig(cfg *SmartgoBrokerConfig) *BrokerConfig {
	brokerConfig := NewDefaultBrokerConfig()
	brokerConfig.BrokerName = cfg.BrokerName
	brokerConfig.BrokerClusterName = cfg.BrokerClusterName
	brokerConfig.AutoCreateTopicEnable = cfg.AutoCreateTopicEnable
	brokerConfig.BrokerId = cfg.BrokerId
	brokerConfig.SmartgoDataPath = cfg.SmartgoDataPath
	brokerConfig.BrokerPort = cfg.BrokerPort
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

// CheckBrokerConfigAttr 校验broker启动的所必需的SmartGoHome、namesrv配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/22
func (self *BrokerConfig) CheckBrokerConfigAttr() bool {
	// 如果没有设置home环境变量，则启动失败
	if "" == self.SmartGoHome {
		format := "please set the '%s' variable in your environment to match the location of the smartgo installation"
		logger.Infof(format, SMARTGO_HOME_ENV)
		return false
	}

	// 检测环境变量NAMESRV_ADDR
	nameSrvAddr := self.NamesrvAddr
	if strings.TrimSpace(nameSrvAddr) == "" {
		format := "please set the '%s' variable in your environment"
		logger.Infof(format, NAMESRV_ADDR_ENV)
		return false
	}

	// 检测NameServer环境变量设置是否正确 IP:PORT
	addrs := strings.Split(strings.TrimSpace(nameSrvAddr), ";")
	if addrs == nil || len(addrs) == 0 {
		format := "the %s=%s environment variable is invalid."
		logger.Infof(format, NAMESRV_ADDR_ENV, addrs)
		return false
	}
	for _, addr := range addrs {
		if !CheckIpAndPort(addr) {
			format := "the name server address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\""
			logger.Infof(format, addr)
			return false
		}
	}

	return true
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

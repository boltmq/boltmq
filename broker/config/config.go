// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package config

import (
	"fmt"

	"github.com/boltmq/common/constant"
)

type Config struct {
	MQHome  string        `toml:"-"`       // BoltMQ安装运行路径
	CfgPath string        `toml:"-"`       // BoltMQ配置文件路径
	Cluster ClusterConfig `toml:"cluster"` // 集群配置
	Broker  BrokerConfig  `toml:"broker"`  // 参数配置
	Store   StoreConfig   `toml:"store"`   // store数据存储目录
	Log     LogConfig     `toml:"log"`     // 日志
}

// ClusterConfig 集群配置
type ClusterConfig struct {
	Name         string   `toml:"name"`          // 集群名称
	BrokerId     int64    `toml:"broker_id"`     // broker id
	BrokerName   string   `toml:"broker_name"`   // broker名称
	BrokerRole   string   `toml:"broker_role"`   // broker角色 主/备
	HaServerIP   string   `toml:"ha_server_ip"`  // 主备配置
	NameSrvAddrs []string `toml:"namesrv_addrs"` // Namesrv地址
}

// BrokerConfig
type BrokerConfig struct {
	Port                               int    `toml:"port"`                                   // broker对外提供服务端口
	IP                                 string `toml:"ip"`                                     // 本机ip地址
	DeleteWhen                         int    `toml:"delete_when"`                            // 何时触发删除无效Message
	AutoCreateTopicEnable              bool   `toml:"auto_create_topic_enable"`               // 是否允许客户端自动创建Topic（生产环境建议关闭）
	Permission                         int    `toml:"permission"`                             // Broker权限
	DefaultTopicQueueNums              int32  `toml:"default_topic_queue_nums"`               // 默认topic队列数
	ClusterTopicEnable                 bool   `toml:"cluster_topic_enable"`                   // 自动创建以集群名字命名的Topic功能是否开启
	BrokerTopicEnable                  bool   `toml:"broker_topic_enable"`                    // 自动创建以服务器名字命名的Topic功能是否开启
	AutoCreateSubscriptionGroup        bool   `toml:"auto_create_subscription_group"`         // 自动创建订阅组功能是否开启（线上建议关闭）
	FlushConsumerOffsetInterval        int    `toml:"flush_consumer_offset_interval"`         // 刷新ConsumerOffest定时间隔
	FlushConsumerOffsetHistoryInterval int    `toml:"flush_consumer_offset_history_interval"` // 此字段目前没有使用
	RejectTransactionMessage           bool   `toml:"reject_transaction_message"`             // 是否拒绝接收事务消息
	FetchNameSrvAddrByAddressServer    bool   `toml:"fetch_namesrv_addr_by_address_server"`   // 是否从地址服务器寻找NameServer地址，正式发布后，默认值为false
	SendThreadPoolQueueCapacity        int    `toml:"send_thread_pool_queue_capacity"`        // 发送消息对应的线程池阻塞队列size
	PullThreadPoolQueueCapacity        int    `toml:"pull_thread_pool_queue_capacity"`        // 订阅消息对应的线程池阻塞队列size
	FilterServerNums                   int32  `toml:"filter_server_nums"`                     // 过滤服务器数量
	LongPollingEnable                  bool   `toml:"long_polling_enable"`                    // Consumer订阅消息时，Broker是否开启长轮询
	ShortPollingTimeMills              int    `toml:"short_polling_timemills"`                // 如果是短轮询，服务器挂起时间
	NotifyConsumerIdsChangedEnable     bool   `toml:"notify_consumer_ids_changed_enable"`     // notify consumerId changed 开关
	OffsetCheckInSlave                 bool   `toml:"offset_check_in_slave"`                  // slave 是否需要纠正位点
	HaMasterAddress                    string `toml:"ha_master_addr"`                         // 适用场景：HA功能配置(将slave角色的 ha地址，指向master角色)
}

// StoreConfig 存储相关配置
type StoreConfig struct {
	RootDir          string `toml:"root_dir"`           // store的数据存储目录
	FlushDiskType    string `toml:"flush_disk_type"`    // 刷盘方式
	FileReservedTime int    `toml:"file_reserved_time"` // 消息保存时间
}

// LogConfig 日志配置
type LogConfig struct {
	CfgFilePath string `toml:"config_file_path"` // 日志配置文件路径
}

// HasReadable 校验Broker是否有读权限
// Author: tianyuliang
// Since: 2017/9/29
func (cfg *Config) HasReadable() bool {
	return constant.IsReadable(cfg.Broker.Permission)
}

func (cfg *Config) HasWriteable() bool {
	return constant.IsWriteable(cfg.Broker.Permission)
}

// String
func (cfg *Config) String() string {
	if cfg == nil {
		return "<nil>"
	}

	return fmt.Sprintf("Config [ClusterName=%s, BrokerName=%s, BrokerId=%d, BrokerRole=%s, DeleteWhen=%d, FileReservedTime=%d, FlushDiskType=%s, AutoCreateTopicEnable=%t, Store.RootDir=%s, Log.CfgFilePath=%s]",
		cfg.Cluster.Name, cfg.Cluster.BrokerName, cfg.Cluster.BrokerId, cfg.Cluster.BrokerRole, cfg.Broker.DeleteWhen,
		cfg.Store.FileReservedTime, cfg.Store.FlushDiskType, cfg.Broker.AutoCreateTopicEnable, cfg.Store.RootDir, cfg.Log.CfgFilePath)
}

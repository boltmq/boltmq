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

import "fmt"

type Config struct {
	Cluster ClusterConfig `toml:"cluster"` // 集群配置
	Store   StoreConfig   `toml:"store"`   // broker、store等模块的数据存储目录
	Log     LogConfig     `toml:"log"`     // 日志
}

// ClusterConfig 集群配置
type ClusterConfig struct {
	ClusterName           string `toml:"cluster_name"`             // 集群名称
	BrokerName            string `toml:"broker_name"`              // broker名称
	BrokerId              int    `toml:"broker_id"`                // broker id
	BrokerRole            string `toml:"broker_role"`              // broker角色 主/备
	DeleteWhen            int    `toml:"delete_when"`              // 何时触发删除无效Message
	FileReservedTime      int    `toml:"file_reserved_time"`       // 消息保存时间
	FlushDiskType         string `toml:"flush_disk_type"`          // 刷盘方式
	AutoCreateTopicEnable bool   `toml:"auto_create_topic_enable"` // 是否允许客户端自动创建Topic
}

// StoreConfig 存储相关配置
type StoreConfig struct {
	RootDir string `toml:"root_dir"` // store的数据存储目录
}

// LogConfig 日志配置
type LogConfig struct {
	CfgFilePath string `toml:"config_file_path"` // 日志配置文件路径
}

// String
func (cfg *Config) String() string {
	if cfg == nil {
		return "<nil>"
	}

	return fmt.Sprintf("Config [ClusterName=%s, BrokerName=%s, BrokerId=%d, BrokerRole=%s, DeleteWhen=%d, FileReservedTime=%d, FlushDiskType=%s, AutoCreateTopicEnable=%t, Store.RootDir=%s, Log.CfgFilePath=%s]",
		cfg.Cluster.ClusterName, cfg.Cluster.BrokerName, cfg.Cluster.BrokerId, cfg.Cluster.BrokerRole, cfg.Cluster.DeleteWhen,
		cfg.Cluster.FileReservedTime, cfg.Cluster.FlushDiskType, cfg.Cluster.AutoCreateTopicEnable, cfg.Store.RootDir, cfg.Log.CfgFilePath)
}

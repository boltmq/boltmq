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
	"os"
	"path/filepath"

	"github.com/boltmq/common/utils/encoding"
	"github.com/flosch/pongo2"
	"github.com/imdario/mergo"
)

// ParseConfig 解析配置文件
func ParseConfig(path string) (*Config, error) {
	var (
		cfg Config
	)

	path = getConfigPath(path, envBoltmqBrokerConfigPath)
	if err := encoding.DecodeToml(path, &cfg); err != nil {
		return nil, err
	}

	if err := mergeConfig(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// 获取配置文件路径。1. 传入参数得到路径
// 2.传入参数为空，从环境变量得到路径。 以上都未得到，返回默认路径。
func getConfigPath(path, envar string) string {
	if path != "" {
		return path
	}

	if path = os.Getenv(envar); path != "" {
		return path
	}

	return defaultValue(envar)
}

var defaultConfig = &Config{
	Cluster: ClusterConfig{
		ClusterName:           "BoltMQCluster",
		BrokerName:            "broker-node",
		BrokerId:              0,
		BrokerRole:            "SYNC_MASTER",
		DeleteWhen:            4,
		FileReservedTime:      48,
		FlushDiskType:         "SYNC_FLUSH",
		AutoCreateTopicEnable: false,
	},
	Store: StoreConfig{
		RootDir: defaultRootDir(),
	},
	Log: LogConfig{
		CfgFilePath: "etc/seelog.xml",
	},
}

func mergeConfig(cfg *Config) error {
	if err := mergo.Merge(cfg, defaultConfig); err != nil {
		return err
	}

	//对路径进行修正
	cfg.Store.RootDir = fixPath(cfg.Store.RootDir)
	cfg.Log.CfgFilePath = fixPath(cfg.Log.CfgFilePath)
	return nil
}

func fixPath(path string) string {
	uhome, err := home()
	if err != nil {
		return path
	}

	tpl, err := pongo2.FromString(path)
	if err != nil {
		return path
	}

	// 替换Home
	out, err := tpl.Execute(pongo2.Context{"HOME": uhome})
	if err != nil {
		return path
	}

	return filepath.FromSlash(out)
}

func defaultRootDir() string {
	uhome, err := home()
	if err != nil {
		return ""
	}

	return fmt.Sprintf("%s%cstore", uhome, os.PathSeparator)
}

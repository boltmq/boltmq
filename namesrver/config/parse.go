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
	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/common/utils/encoding"
	"github.com/imdario/mergo"
)

// ParseConfig 解析配置文件
func ParseConfig(path string) (*Config, error) {
	var (
		cfg Config
	)

	path = common.GetConfigEnvValue(path, common.EnvBoltMQNameSrvConfigPath)
	if err := encoding.DecodeToml(path, &cfg); err != nil {
		return nil, err
	}

	if err := mergeConfig(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

var defaultConfig = &Config{
	NameSrv: NameSrvConfig{
		Host: "0.0.0.0",
		Port: 9876,
	},
	Log: LogConfig{
		CfgFilePath: "etc/seelog-nsrv.xml",
	},
}

func mergeConfig(cfg *Config) error {
	if err := mergo.Merge(cfg, defaultConfig); err != nil {
		return err
	}

	return nil
}

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
package common

import (
	"os"
)

const (
	EnvBoltMQBrokerConfigPath  = "BOLTMQ_BROKER_CONFIG_PATH"
	EnvNameSrvAddrs            = "NAMESRV_ADDRS"
	EnvBoltMQHome              = "BOLTMQ_HOME"
	EnvBoltMQNameSrvConfigPath = "BOLTMQ_NAMESRV_CONFIG_PATH"
)

var envNullDefaultValues = map[string]string{
	EnvBoltMQBrokerConfigPath:  "etc/broker.toml",
	EnvNameSrvAddrs:            "127.0.0.1:9876",
	EnvBoltMQHome:              "",
	EnvBoltMQNameSrvConfigPath: "etc/namesrv.toml",
}

// 获取配置文件路径。1. 传入参数得到路径
// 2.传入参数为空，从环境变量得到路径。 以上都未得到，返回默认路径。
func GetConfigEnvValue(val, envar string) string {
	if val != "" {
		return val
	}

	if val = os.Getenv(envar); val != "" {
		return val
	}

	return defaultValue(envar)
}

func defaultValue(envar string) string {
	if v, ok := envNullDefaultValues[envar]; ok {
		return v
	}

	return ""
}

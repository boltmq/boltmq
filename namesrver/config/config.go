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

type Config struct {
	NameSrv NameSrvConfig `toml:"namesrv"` // 参数配置
	Log     LogConfig     `toml:"log"`     // 日志
}

// NameSrvConfig namesrv相关配置
type NameSrvConfig struct {
	Host      string `toml:"host"`           // 监听地址
	Port      int    `toml:"port"`           // 监听端口
	KVCfgPath string `toml:"kv_config_path"` // kv文件存储路径
}

// LogConfig 日志配置
type LogConfig struct {
	CfgFilePath string `toml:"config_file_path"` // 日志配置文件路径
}

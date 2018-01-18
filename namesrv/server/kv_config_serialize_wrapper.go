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
package server

// KVConfigSerializeWrapper KV配置的json序列化结构
// Author: tianyuliang
// Since: 2017/9/4
type KVConfigSerializeWrapper struct {
	ConfigTable map[string]map[string]string `json:"configTable"` // 数据格式：Namespace[Key[Value]]
}

// NewKVConfigSerializeWrapper 初始化KV配置
// Author: tianyuliang
// Since: 2017/9/4
func NewKVConfigSerializeWrapper(configTable map[string]map[string]string) *KVConfigSerializeWrapper {
	kvConfigWrapper := &KVConfigSerializeWrapper{
		ConfigTable: configTable,
	}
	return kvConfigWrapper
}

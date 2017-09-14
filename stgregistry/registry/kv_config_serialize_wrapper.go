package registry

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// KVConfigSerializeWrapper KV配置的json序列化结构
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
type KVConfigSerializeWrapper struct {
	ConfigTable map[string]map[string]string // 数据格式：Namespace[Key[Value]]
	*protocol.RemotingSerializable
}

// NewKVConfigSerializeWrapper 初始化KV配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
func NewKVConfigSerializeWrapper(configTable map[string]map[string]string) *KVConfigSerializeWrapper {
	kvConfigSerializeWrapper := &KVConfigSerializeWrapper{
		ConfigTable: configTable,
	}
	return kvConfigSerializeWrapper
}

package namesrv

// KVConfigSerializeWrapper KV配置的json序列化结构
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
type KVConfigSerializeWrapper struct {
	ConfigTable map[string]map[string]string // Namespace[Key[Value]]
}

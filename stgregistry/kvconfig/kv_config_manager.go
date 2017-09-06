package kvconfig

import (
	"sync"
)

var (
	configTable   KVConfigSerializeWrapper
	ReadWriteLock sync.RWMutex
)

// printAllPeriodically 打印namesrv全局配置信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigSerializeWrapper) printAllPeriodically() {

}

// persist 将内存中的namesrv配置项持久化到kvConfig.json文件
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigSerializeWrapper) persist() {

}

// deleteKVConfigByValue 从指定Namespace配置中，根据value，删除对应的key键
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigSerializeWrapper) deleteKVConfigByValue(namespace, value string) {

}

// getKVConfigByValue 从指定Namespace配置中，根据value，反向查找key列表，并将key列表通过分号;拼接为字符串
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigSerializeWrapper) getKVConfigByValue(namespace, value string) string {
	return ""
}

// getKVConfig 从指定Namespace配置中，根据key获取value值
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigSerializeWrapper) getKVConfig(namespace, key string) string {
	return ""
}

// getKVListByNamespace 获取指定Namespace所有的KV配置List
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigSerializeWrapper) getKVListByNamespace(namespace string) []byte {
	return []byte{}
}

// deleteKVConfig 从Namesrv配置列表中，根据key删除对应的键值对
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigSerializeWrapper) deleteKVConfig(namespace, key string) {

}

// putKVConfig 向Namesrv追加KV配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigSerializeWrapper) putKVConfig(namespace, key string) {

}

//
// load 加载kvConfig.json至KVConfigManager的configTable，即持久化转移到内存
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigSerializeWrapper) load() {

}

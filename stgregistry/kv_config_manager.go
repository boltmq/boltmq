package stgregistry

import (
	"sync"
)

// KVConfigManager KV配置管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
type KVConfigManager struct {
	ConfigTable       *KVConfigSerializeWrapper
	ReadWriteLock     sync.RWMutex
	// NamesrvController *DefaultNamesrvController // 暂时不需要
}

// NewKVConfigManager 初始化KV配置管理器
// // NamesrvController *stgregistry.DefaultNamesrvController
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func NewKVConfigManager() *KVConfigManager {
	kvConfigManager := &KVConfigManager{
		ConfigTable: new(KVConfigSerializeWrapper),
	}
	return kvConfigManager
}

// printAllPeriodically 打印namesrv全局配置信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) printAllPeriodically() {

}

// persist 将内存中的namesrv配置项持久化到kvConfig.json文件
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) persist() {

}

// deleteKVConfigByValue 从指定Namespace配置中，根据value，删除对应的key键
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) deleteKVConfigByValue(namespace, value string) {

}

// getKVConfigByValue 从指定Namespace配置中，根据value，反向查找key列表，并将key列表通过分号;拼接为字符串
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) getKVConfigByValue(namespace, value string) string {
	return ""
}

// getKVConfig 从指定Namespace配置中，根据key获取value值
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) getKVConfig(namespace, key string) string {
	self.ReadWriteLock.RLock()
	if kvTable, ok := self.ConfigTable.ConfigTable[namespace]; ok && kvTable != nil {
		if value, ok := kvTable[key]; ok {
			return value
		}
	}
	self.ReadWriteLock.RUnlock()
	return ""
}

// getKVListByNamespace 获取指定Namespace所有的KV配置List
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) getKVListByNamespace(namespace string) []byte {
	return []byte{}
}

// deleteKVConfig 从Namesrv配置列表中，根据key删除对应的键值对
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) deleteKVConfig(namespace, key string) {

}

// putKVConfig 向Namesrv追加KV配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) putKVConfig(namespace, key string) {

}

// load 加载kvConfig.json至KVConfigManager的configTable，即持久化转移到内存
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) load() error {
	return nil
}

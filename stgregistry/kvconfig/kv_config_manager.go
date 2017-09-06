package kvconfig

import (
	"git.oschina.net/cloudzone/smartgo/stgregistry/controller"
	"sync"
)

var (
	configTable   *KVConfigSerializeWrapper
	readWriteLock *sync.RWMutex
)

type KVConfigManager struct {
	ConfigTable       *KVConfigSerializeWrapper
	ReadWriteLock     *sync.RWMutex
	NamesrvController *controller.NamesrvController
}

// NewKVConfigManager 初始化KV配置管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func NewKVConfigManager(namesrvController *controller.NamesrvController) *KVConfigManager {
	kvConfigManager := KVConfigManager{
		NamesrvController: namesrvController,
	}
	return &kvConfigManager
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
func (self *KVConfigManager) load() {

}

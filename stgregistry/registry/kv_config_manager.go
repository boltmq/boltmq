package registry

import (
	"bytes"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgregistry/logger"
	"strings"
	"sync"
)

// KVConfigManager KV配置管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
type KVConfigManager struct {
	ConfigTable       map[string]map[string]string // 数据格式：Namespace[Key[Value]]
	ReadWriteLock     sync.RWMutex
	NamesrvController *DefaultNamesrvController
}

// NewKVConfigManager 初始化KV配置管理器
// // NamesrvController *stgregistry.DefaultNamesrvController
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func NewKVConfigManager(controller *DefaultNamesrvController) *KVConfigManager {
	kvConfigManager := &KVConfigManager{
		ConfigTable:       make(map[string]map[string]string),
		NamesrvController: controller,
	}
	return kvConfigManager
}

// printAllPeriodically 打印namesrv全局配置信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) printAllPeriodically() {
	self.ReadWriteLock.RLock()
	logger.Info("--------------------------------------------------------")
	logger.Info("configTable size: %d", len(self.ConfigTable))
	if self.ConfigTable != nil {
		for namespace, kvTable := range self.ConfigTable {
			if kvTable != nil {
				for key, value := range kvTable {
					logger.Info("configTable Namespace: %s Key: %s Value: %s", namespace, key, value)
				}
			}
		}
	}
	self.ReadWriteLock.RUnlock()
}

// persist 将内存中的namesrv配置项持久化到kvConfig.json文件
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) persist() {
	self.ReadWriteLock.RLock()
	kvConfigSerializeWrapper := NewKVConfigSerializeWrapper(self.ConfigTable)
	content := kvConfigSerializeWrapper.CustomEncode(kvConfigSerializeWrapper)
	if content != nil && len(content) > 0 {
		kvConfigPath := self.NamesrvController.NamesrvConfig.GetKvConfigPath()
		stgcommon.String2File(content, kvConfigPath)
	}
	self.ReadWriteLock.RUnlock()
}

// deleteKVConfigByValue 从指定Namespace配置中，根据value，删除对应的key键
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) deleteKVConfigByValue(namespace, value string) {
	self.ReadWriteLock.Lock()
	if kvTable, ok := self.ConfigTable[namespace]; ok && kvTable != nil {
		cloneKvTable := make(map[string]string)
		for k, v := range kvTable {
			cloneKvTable[k] = v
		}

		for k, v := range cloneKvTable {
			if v == value {
				delete(kvTable, k)
				format := "deleteIpsByProjectGroup delete a config item, Namespace: %s Key: %s Value: %s"
				logger.Info(format, namespace, k, v)
			}
		}
	}
	self.ReadWriteLock.Unlock()
	self.persist()
}

// getKVConfigByValue 从指定Namespace配置中，根据value，反向查找key列表，并将key列表通过分号;拼接为字符串
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) getKVConfigByValue(namespace, value string) string {
	self.ReadWriteLock.RLock()
	if kvTable, ok := self.ConfigTable[namespace]; ok && kvTable != nil {
		buf := new(bytes.Buffer)
		splitor := ""
		for k, v := range kvTable {
			if v == value {
				buf.WriteString(splitor)
				buf.WriteString(k)
				splitor = ";"
			}
		}
		return buf.String()
	}
	self.ReadWriteLock.RUnlock()
	return ""
}

// getKVConfig 从指定Namespace配置中，根据key获取value值
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) getKVConfig(namespace, key string) string {
	self.ReadWriteLock.RLock()
	logger.Info("params[namespace=%s, key=%s], map:%s", namespace, key, self.ConfigTable)
	if kvTable, ok := self.ConfigTable[namespace]; ok && kvTable != nil {
		logger.Info("kvTable --> %s", self.ConfigTable[namespace])
		if value, ok := kvTable[key]; ok {
			logger.Info("value --> %s", self.ConfigTable[namespace][key])
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
	self.ReadWriteLock.RLock()
	if kvTable, ok := self.ConfigTable[namespace]; ok && kvTable != nil {
		tb := &body.KVTable{}
		tb.Table = kvTable
		return tb.CustomEncode(tb)
	}
	self.ReadWriteLock.RUnlock()

	return []byte{}
}

// deleteKVConfig 从Namesrv配置列表中，根据key删除对应的键值对
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) deleteKVConfig(namespace, key string) {
	self.ReadWriteLock.Lock()
	if kvTable, ok := self.ConfigTable[namespace]; ok && kvTable != nil {
		format := "deleteKVConfig delete a config item, Namespace: %s Key: %s Value: %s"
		value, _ := kvTable[key]
		logger.Info(format, namespace, key, value)
		delete(kvTable, key)
	}
	self.ReadWriteLock.Unlock()

	self.persist()
}

// putKVConfig 向Namesrv追加KV配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) putKVConfig(namespace, key, value string) {
	self.ReadWriteLock.Lock()
	kvTable, ok := self.ConfigTable[namespace]
	if !ok || kvTable == nil {
		kvTable = make(map[string]string)
		self.ConfigTable[namespace] = kvTable
	}

	// 检查key是否已存在
	format := "putKVConfig update config item, Namespace: %s Key: %s Value: %s"
	if _, ok := kvTable[key]; !ok {
		format = "putKVConfig create new config item, Namespace: %s Key: %s Value: %s"
	}
	logger.Info(format, namespace, key, value)
	kvTable[key] = value
	self.ReadWriteLock.Unlock()

	self.persist()
}

// load 加载kvConfig.json至KVConfigManager的configTable，即持久化转移到内存
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) load() error {
	// 如果kvConfig.json文件不存在，则创建
	cfgPath := self.NamesrvController.NamesrvConfig.GetKvConfigPath()
	logger.Info("get kvConfigPath = %s", cfgPath)
	cfgName := self.NamesrvController.NamesrvConfig.GetKvConfigName()
	if ok, err := stgcommon.ExistsFile(cfgPath); err != nil || !ok {
		ok, err := stgcommon.CreateFile(cfgPath)
		if err != nil {
			return fmt.Errorf("create %s failed. err: %s", cfgName, err.Error())
		}
		if !ok {
			return fmt.Errorf("create %s failed, but err is nil", cfgName)
		}
		logger.Info("create %s successful.", cfgName)
	}

	// 读取kvConfig.json文件内容，并打印日志
	content, err := stgcommon.File2String(cfgPath)
	if err != nil {
		return fmt.Errorf("load %s error: %s", cfgName, err.Error())
	}
	val := content
	if val == "" {
		val = "is empty"
	}
	logger.Info("read %s successful. content %s", cfgName, val)

	if strings.TrimSpace(content) != "" {
		// kvConfig.json文件内容有数据，则反序列化为KVConfigSerializeWrapper
		buf := []byte(strings.TrimSpace(content))
		kvConfigSerializeWrapper := new(KVConfigSerializeWrapper)
		err := kvConfigSerializeWrapper.CustomDecode(buf, kvConfigSerializeWrapper)
		if err != nil {
			return fmt.Errorf("kvConfigSerializeWrapper decode err: %s \n\t %s", err.Error(), content)
		}
		if kvConfigSerializeWrapper != nil && kvConfigSerializeWrapper.ConfigTable != nil {
			for k, v := range kvConfigSerializeWrapper.ConfigTable {
				self.ConfigTable[k] = v
			}
		}
	}
	logger.Info("kvConfigManager load successful.")
	return nil
}

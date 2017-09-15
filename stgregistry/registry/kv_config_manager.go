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
	if kvTable, ok := self.ConfigTable[namespace]; ok && kvTable != nil {
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
	if kvTable, ok := self.ConfigTable[namespace]; ok {
		if kvTable == nil {
			kvTable = make(map[string]string)
			self.ConfigTable[namespace] = kvTable
		}

		// 检查是否存在oldValue
		format := "putKVConfig update config item, Namespace: %s Key: %s Value: %s"
		if oldValue, ok := kvTable[key]; ok && oldValue == "" {
			// String prev = kvTable.put(key, value) && prev == null
			format = "putKVConfig create new config item, Namespace: %s Key: %s Value: %s"
		}
		logger.Info(format, namespace, key, value)
		kvTable[key] = value
	}
	self.ReadWriteLock.Unlock()

	self.persist()
}

// load 加载kvConfig.json至KVConfigManager的configTable，即持久化转移到内存
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *KVConfigManager) load() error {
	kvConfigPath := self.NamesrvController.NamesrvConfig.GetKvConfigPath()
	// 加载文件之前，如果文件不存在，则创建
	if ok, err := stgcommon.ExistsFile(kvConfigPath); err != nil || !ok {
		if ok, err := stgcommon.CreateDir(kvConfigPath); err != nil || !ok {
			return fmt.Errorf("创建kvConfig配置文件异常. kvConfigPath=%s", kvConfigPath)
		}
	}

	// 读取文件内容
	content, err := stgcommon.File2String(kvConfigPath)
	if err != nil {
		logger.Error("kvConfigManager load error: %s \n\t%s", kvConfigPath, err.Error())
		return err
	}
	values := strings.Split(kvConfigPath, "/")
	kvConfigName := values[len(values)-1]

	logData := content
	if logData == "" {
		logData = "is empty"
	}
	logger.Info("read %s successful. content %s", kvConfigName, logData)

	if strings.TrimSpace(content) != "" {
		// 文件内容有数据，反序列化为KVConfigSerializeWrapper
		buf := []byte(content)
		var kvConfigSerializeWrapper *KVConfigSerializeWrapper
		err := kvConfigSerializeWrapper.CustomDecode(buf, kvConfigSerializeWrapper)
		if err != nil {
			return fmt.Errorf("kvConfigSerializeWrapper decode err: %s", err.Error())
		}
		if kvConfigSerializeWrapper != nil && kvConfigSerializeWrapper.ConfigTable != nil {
			for k, v := range kvConfigSerializeWrapper.ConfigTable {
				self.ConfigTable[k] = v
			}
		}
	}
	logger.Info("kvConfigManager load end ... ")
	return nil
}

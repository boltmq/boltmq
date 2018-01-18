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

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/protocol"
)

// kvConfigManager KV配置管理器
// Author: tianyuliang
// Since: 2017/9/8
type kvConfigManager struct {
	configTable map[string]map[string]string // 数据格式：Namespace[Key[Value]]
	rwLock      sync.RWMutex
	controller  *NameSrvController
}

// newKVConfigManager 初始化KV配置管理器
// Author: tianyuliang
// Since: 2017/9/6
func newKVConfigManager(controller *NameSrvController) *kvConfigManager {
	kvCfgManager := &kvConfigManager{
		configTable: make(map[string]map[string]string),
		controller:  controller,
	}
	return kvCfgManager
}

// printAllPeriodically 打印namesrv全局配置信息
// Author: tianyuliang
// Since: 2017/9/6
func (kvCfg *kvConfigManager) printAllPeriodically() {
	kvCfg.rwLock.RLock()
	defer kvCfg.rwLock.RUnlock()

	if kvCfg.configTable != nil && len(kvCfg.configTable) > 0 {
		logger.Info("configTable size: %d", len(kvCfg.configTable))
		for namespace, kvTable := range kvCfg.configTable {
			if kvTable != nil {
				for key, value := range kvTable {
					logger.Info("namespace=%s, key=%s, value=%s", namespace, key, value)
				}
			}
		}
	}
}

// persist 将内存中的namesrv配置项持久化到kvConfig.json文件
// Author: tianyuliang
// Since: 2017/9/6
func (kvCfg *kvConfigManager) persist() {
	kvCfg.rwLock.RLock()
	kvConfigWrapper := NewKVConfigSerializeWrapper(kvCfg.configTable)
	content, err := common.Encode(kvConfigWrapper)
	if err != nil {
		logger.Errorf("kv config encode failed, err: %s.", err)
		return
	}

	kvConfigPath := kvCfg.controller.cfg.NameSrv.KVCfgPath
	common.String2File(content, kvConfigPath)
	logger.Info("persist kv config success.")

	kvCfg.rwLock.RUnlock()
}

// deleteKVConfigByValue 从指定Namespace配置中，根据value，删除对应的key键
// Author: tianyuliang
// Since: 2017/9/6
func (kvCfg *kvConfigManager) deleteKVConfigByValue(namespace, value string) {
	kvCfg.rwLock.Lock()
	if kvTable, ok := kvCfg.configTable[namespace]; ok && kvTable != nil {
		cloneKvTable := make(map[string]string)
		for k, v := range kvTable {
			cloneKvTable[k] = v
		}

		for k, v := range cloneKvTable {
			if v == value {
				delete(kvTable, k)
				format := "delete ips by project group delete a config item, Namespace: %s Key: %s Value: %s"
				logger.Info(format, namespace, k, v)
			}
		}
	}
	kvCfg.rwLock.Unlock()
	kvCfg.persist()
}

// getKVConfigByValue 从指定Namespace配置中，根据value，反向查找key列表，并将key列表通过分号;拼接为字符串
// Author: tianyuliang
// Since: 2017/9/6
func (kvCfg *kvConfigManager) getKVConfigByValue(namespace, value string) string {
	kvCfg.rwLock.RLock()
	if kvTable, ok := kvCfg.configTable[namespace]; ok && kvTable != nil {
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
	kvCfg.rwLock.RUnlock()
	return ""
}

// getKVConfig 从指定Namespace配置中，根据key获取value值
// Author: tianyuliang
// Since: 2017/9/6
func (kvCfg *kvConfigManager) getKVConfig(namespace, key string) string {
	kvCfg.rwLock.RLock()
	if kvTable, ok := kvCfg.configTable[namespace]; ok && kvTable != nil {
		if value, ok := kvTable[key]; ok {
			return value
		}
	}
	kvCfg.rwLock.RUnlock()
	return ""
}

// getKVListByNamespace 获取指定Namespace所有的KV配置List
// Author: tianyuliang
// Since: 2017/9/6
func (kvCfg *kvConfigManager) getKVListByNamespace(namespace string) []byte {
	kvCfg.rwLock.RLock()
	if kvTable, ok := kvCfg.configTable[namespace]; ok && kvTable != nil {
		tb := protocol.NewKVTable()
		for topic, value := range kvTable {
			tb.Table[topic] = value
		}

		buf, err := common.Encode(tb)
		if err != nil {
			return nil
		}

		return buf
	}
	kvCfg.rwLock.RUnlock()

	return []byte{}
}

// deleteKVConfig 从Namesrv配置列表中，根据key删除对应的键值对
// Author: tianyuliang
// Since: 2017/9/6
func (kvCfg *kvConfigManager) deleteKVConfig(namespace, key string) {
	kvCfg.rwLock.Lock()
	if kvTable, ok := kvCfg.configTable[namespace]; ok && kvTable != nil {
		format := "delete a config item, Namespace: %s Key: %s Value: %s"
		value, _ := kvTable[key]
		logger.Info(format, namespace, key, value)
		delete(kvTable, key)
	}
	kvCfg.rwLock.Unlock()

	kvCfg.persist()
}

// putKVConfig 向Namesrv追加KV配置
// Author: tianyuliang
// Since: 2017/9/6
func (kvCfg *kvConfigManager) putKVConfig(namespace, key, value string) {
	var (
		exist bool
	)

	kvCfg.rwLock.Lock()
	kvTable, ok := kvCfg.configTable[namespace]
	if !ok || kvTable == nil {
		kvTable = make(map[string]string)
		kvCfg.configTable[namespace] = kvTable
	}

	_, exist = kvTable[key]
	// 检查key是否已存在
	kvTable[key] = value
	kvCfg.rwLock.Unlock()

	if exist {
		logger.Info("putKVConfig update new config item, Namespace: %s Key: %s Value: %s.",
			namespace, key, value)
	} else {
		logger.Info("putKVConfig create new config item, Namespace: %s Key: %s Value: %s.",
			namespace, key, value)
	}

	kvCfg.persist()
}

// load 加载kvConfig.json至kvConfigManager的configTable，即持久化转移到内存
// Author: tianyuliang
// Since: 2017/9/6
func (kvCfg *kvConfigManager) load() error {
	// 如果kvConfig.json文件不存在，则创建
	cfgPath := kvCfg.controller.cfg.NameSrv.KVCfgPath
	cfgName := kvCfg.controller.cfg.NameSrv.KVCfgPath

	ok, err := common.PathExists(cfgPath)
	if err != nil {
		return err
	}

	if !ok {
		ok, err := common.CreateNullFile(cfgPath)
		if err != nil {
			return fmt.Errorf("create %s failed. err: %s.", cfgName, err)
		}
		if !ok {
			return fmt.Errorf("create %s failed, but err is nil", cfgName)
		}
		logger.Infof("create %s success", cfgName)
	}

	// 读取kvConfig.json文件内容，并打印日志
	content, err := common.File2String(cfgPath)
	if err != nil {
		return fmt.Errorf("load %s error: %s", cfgName, err)
	}
	val := content
	if val == "" {
		val = "is empty"
	}
	logger.Infof("read %s success. content %s", cfgName, val)

	if strings.TrimSpace(content) != "" {
		// kvConfig.json文件内容有数据，则反序列化为KVConfigSerializeWrapper
		buf := []byte(strings.TrimSpace(content))
		kvConfigWrapper := new(KVConfigSerializeWrapper)
		err := common.Decode(buf, kvConfigWrapper)
		if err != nil {
			return fmt.Errorf("kv config load decode err: %s \n\t %s", err, content)
		}
		if kvConfigWrapper == nil || kvConfigWrapper.ConfigTable == nil {
			return fmt.Errorf("kv config is nil")
		}
		for k, v := range kvConfigWrapper.ConfigTable {
			kvCfg.configTable[k] = v
		}
		logger.Info("set configTable from %s to kvCfg", cfgName)
	}

	logger.Info("kv config manager load success.")
	return nil
}

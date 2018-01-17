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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/constant"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/base"
	set "github.com/deckarep/golang-set"
)

type topicConfigManager struct {
	brokerController     *BrokerController
	tpCfgSerialWrapper   *base.TopicConfigSerializeWrapper
	lockTopicConfigTable sync.Mutex
	systemTopicList      set.Set
	cfgManagerLoader     *configManagerLoader
	dataVersion          *basis.DataVersion
	lockTimeoutMillis    int64
}

// newTopicConfigManager 初始化topicConfigManager
// Author gaoyanlei
// Since 2017/8/9
func newTopicConfigManager(brokerController *BrokerController) *topicConfigManager {
	var tcm = new(topicConfigManager)
	tcm.brokerController = brokerController
	tcm.tpCfgSerialWrapper = base.NewTopicConfigSerializeWrapper()
	tcm.systemTopicList = set.NewSet()
	tcm.init()
	tcm.cfgManagerLoader = newConfigManagerLoader(tcm)
	tcm.dataVersion = basis.NewDataVersion()
	return tcm
}

func (tcm *topicConfigManager) init() {

	// SELF_TEST_TOPIC
	{
		topicName := basis.SELF_TEST_TOPIC
		topicConfig := base.NewTopicConfig(topicName)
		tcm.systemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1
		topicConfig.WriteQueueNums = 1
		tcm.tpCfgSerialWrapper.TpConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// DEFAULT_TOPIC
	{
		autoCreateTopicEnable := tcm.brokerController.cfg.Broker.AutoCreateTopicEnable
		logger.Infof("BoltMQ set config: AutoCreateTopicEnable=%t.", autoCreateTopicEnable)
		if autoCreateTopicEnable {
			topicName := basis.DEFAULT_TOPIC
			topicConfig := base.NewTopicConfig(topicName)
			tcm.systemTopicList.Add(topicConfig)
			topicConfig.ReadQueueNums = tcm.brokerController.cfg.Broker.DefaultTopicQueueNums
			topicConfig.WriteQueueNums = tcm.brokerController.cfg.Broker.DefaultTopicQueueNums
			topicConfig.Perm = constant.PERM_INHERIT | constant.PERM_READ | constant.PERM_WRITE
			tcm.tpCfgSerialWrapper.TpConfigTable.Put(topicConfig.TopicName, topicConfig)
		}
	}

	// BENCHMARK_TOPIC
	{
		topicName := basis.BENCHMARK_TOPIC
		topicConfig := base.NewTopicConfig(topicName)
		tcm.systemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1024
		topicConfig.WriteQueueNums = 1024
		tcm.tpCfgSerialWrapper.TpConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// DefaultCluster
	{
		topicName := tcm.brokerController.cfg.Cluster.Name
		topicConfig := base.NewTopicConfig(topicName)
		tcm.systemTopicList.Add(topicConfig)
		perm := constant.PERM_INHERIT
		if tcm.brokerController.cfg.Broker.ClusterTopicEnable {
			perm |= constant.PERM_READ | constant.PERM_WRITE
		}
		topicConfig.Perm = perm
		tcm.tpCfgSerialWrapper.TpConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// DEFAULT_BROKER
	{
		topicName := tcm.brokerController.cfg.Cluster.BrokerName
		topicConfig := base.NewTopicConfig(topicName)
		tcm.systemTopicList.Add(topicConfig)
		perm := constant.PERM_INHERIT
		if tcm.brokerController.cfg.Broker.BrokerTopicEnable {
			perm |= constant.PERM_READ | constant.PERM_WRITE
		}
		topicConfig.Perm = perm
		topicConfig.WriteQueueNums = 1
		topicConfig.ReadQueueNums = 1
		tcm.tpCfgSerialWrapper.TpConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// OFFSET_MOVED_EVENT
	{
		topicName := basis.OFFSET_MOVED_EVENT
		topicConfig := base.NewTopicConfig(topicName)
		tcm.systemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1
		topicConfig.WriteQueueNums = 1
		tcm.tpCfgSerialWrapper.TpConfigTable.Put(topicConfig.TopicName, topicConfig)
	}
}

func (tcm *topicConfigManager) isSystemTopic(topic string) bool {
	return tcm.systemTopicList.Contains(topic)
}

func (tcm *topicConfigManager) isTopicCanSendMessage(topic string) bool {
	if topic == basis.DEFAULT_TOPIC || topic == tcm.brokerController.cfg.Cluster.Name {
		return false
	}
	return true
}

// selectTopicConfig 根据topic查找
// Author gaoyanlei
// Since 2017/8/11
func (tcm *topicConfigManager) selectTopicConfig(topic string) *base.TopicConfig {
	topicConfig := tcm.tpCfgSerialWrapper.TpConfigTable.Get(topic)
	if topicConfig != nil {
		return topicConfig
	}
	return nil
}

// createTopicInSendMessageMethod 创建topic
// Author gaoyanlei
// Since 2017/8/10
func (tcm *topicConfigManager) createTopicInSendMessageMethod(topic, defaultTopic,
	remoteAddress string, clientDefaultTopicQueueNums int32, topicSysFlag int) (topicConfig *base.TopicConfig, err error) {

	tcm.lockTopicConfigTable.Lock()
	defer tcm.lockTopicConfigTable.Unlock()

	tc := tcm.tpCfgSerialWrapper.TpConfigTable.Get(topic)
	// 如果获取到topic并且没有出错则反会topicConig
	if tc != nil {
		return tc, nil
	}

	// 是否新创建topic
	createNew := false
	autoCreateTopicEnable := tcm.brokerController.cfg.Broker.AutoCreateTopicEnable

	// 如果通过topic获取不到topic或者服务器不允许自动创建 则直接返回
	if tc == nil && !autoCreateTopicEnable {
		return nil, errors.New("No permissions to create topic")
	}

	// 如果没有获取到topic，服务允许自动创建
	defTopicConfig := tcm.tpCfgSerialWrapper.TpConfigTable.Get(defaultTopic)
	if defTopicConfig != nil {
		if constant.IsInherited(defTopicConfig.Perm) {
			// 设置queueNums个数
			var queueNums int32
			if clientDefaultTopicQueueNums > defTopicConfig.WriteQueueNums {
				queueNums = defTopicConfig.WriteQueueNums
			} else {
				queueNums = clientDefaultTopicQueueNums
			}
			if queueNums < 0 {
				queueNums = 0
			}
			perm := defTopicConfig.Perm
			perm &= 0xFFFFFFFF ^ constant.PERM_INHERIT
			topicConfig = &base.TopicConfig{
				TopicName:      topic,
				WriteQueueNums: queueNums,
				ReadQueueNums:  queueNums,
				TopicSysFlag:   topicSysFlag,
				TpFilterType:   defTopicConfig.TpFilterType,
			}
		} else {
			return nil, errors.New("No permissions to create topic")
		}
	} else {
		return nil, errors.New("create new topic failed, because the default topic not exit")
	}

	if topicConfig != nil {
		tcm.tpCfgSerialWrapper.TpConfigTable.Put(topic, topicConfig)
		tcm.tpCfgSerialWrapper.DataVersion.NextVersion()
		createNew = true
		tcm.cfgManagerLoader.persist()
	}

	// 如果为新建则向所有Broker注册
	if createNew {
		tcm.brokerController.registerBrokerAll(false, true)
	}
	return
}

// createTopicInSendMessageBackMethod 该方法没有判断broker权限.
// Author gaoyanlei
// Since 2017/8/11
func (tcm *topicConfigManager) createTopicInSendMessageBackMethod(topic string, clientDefaultTopicQueueNums int32, perm,
	topicSysFlag int) (topicConfig *base.TopicConfig, err error) {
	tcm.lockTopicConfigTable.Lock()
	defer tcm.lockTopicConfigTable.Unlock()

	tc := tcm.tpCfgSerialWrapper.TpConfigTable.Get(topic)
	// 如果获取到topic并且没有出错则反会topicConig
	if tc != nil {
		return tc, nil
	}

	// 是否新创建topic
	createNew := false

	topicConfig = &base.TopicConfig{
		TopicName:      topic,
		WriteQueueNums: clientDefaultTopicQueueNums,
		ReadQueueNums:  clientDefaultTopicQueueNums,
		TopicSysFlag:   topicSysFlag,
		Perm:           perm,
	}
	tcm.tpCfgSerialWrapper.TpConfigTable.Put(topic, topicConfig)
	tcm.tpCfgSerialWrapper.DataVersion.NextVersion()
	createNew = true
	tcm.cfgManagerLoader.persist()

	// 如果为新建则向所有Broker注册
	if createNew {
		tcm.brokerController.registerBrokerAll(false, true)
	}
	return
}

// updateTopicConfig 更新topic信息
// Author gaoyanlei
// Since 2017/8/10
func (tcm *topicConfigManager) updateTopicConfig(topicConfig *base.TopicConfig) {
	old := tcm.tpCfgSerialWrapper.TpConfigTable.Put(topicConfig.TopicName, topicConfig)
	if old != nil {
		logger.Infof("update topic config, old:%s, new:%s.", old, topicConfig)
	} else {
		logger.Infof("create new topic: %s.", topicConfig)
	}
	tcm.tpCfgSerialWrapper.DataVersion.NextVersion()
	tcm.cfgManagerLoader.persist()
}

// updateOrderTopicConfig 更新顺序topic
// Author gaoyanlei
// Since 2017/8/11
func (tcm *topicConfigManager) updateOrderTopicConfig(orderKVTable *protocol.KVTable) {
	if orderKVTable == nil || orderKVTable.Table == nil {
		logger.Info("orderKVTable or orderKVTable.Table is nil.")
		return
	}

	isChange := false
	orderTopics := set.NewSet()
	// 通过set集合去重
	for k, _ := range orderKVTable.Table {
		orderTopics.Add(k)
	}

	// set遍历
	for val := range orderTopics.Iter() {
		if value, ok := val.(string); ok {
			topicConfig := tcm.tpCfgSerialWrapper.TpConfigTable.Get(value)
			if topicConfig != nil && !topicConfig.Order {
				topicConfig.Order = true
				isChange = true
				logger.Infof("update order topic config, topic=%s, order=%t.", value, true)
			}
		}
	}

	tcm.tpCfgSerialWrapper.TpConfigTable.ForeachUpdate(func(topic string, topicConfig *base.TopicConfig) {
		if !orderTopics.Contains(topic) {
			if topicConfig != nil && topicConfig.Order {
				topicConfig.Order = false
				isChange = true
				logger.Infof("update order topic config, topic=%s, order=%t.", topic, true)
			}
		}
	})

	if isChange {
		tcm.tpCfgSerialWrapper.DataVersion.NextVersion()
		tcm.cfgManagerLoader.persist()
	}
}

// isOrderTopic 判断是否是顺序topic
// Author gaoyanlei
// Since 2017/8/10
func (tcm *topicConfigManager) isOrderTopic(topic string) bool {
	topicConfig := tcm.tpCfgSerialWrapper.TpConfigTable.Get(topic)
	if topicConfig == nil {
		return false
	}
	return topicConfig.Order
}

// deleteTopicConfig 删除topic
// Author gaoyanlei
// Since 2017/8/10
func (tcm *topicConfigManager) deleteTopicConfig(topic string) {
	value := tcm.tpCfgSerialWrapper.TpConfigTable.Remove(topic)
	if value != nil {
		logger.Infof("delete topic config success, topic=%s.", value)
		tcm.tpCfgSerialWrapper.DataVersion.NextVersion()
		tcm.cfgManagerLoader.persist()
	} else {
		logger.Infof("delete topic config failed, topic=%s not exist.", topic)
	}
}

// buildTopicConfigSerializeWrapper 创建tpCfgSerialWrapper
// Author gaoyanlei
// Since 2017/8/11
func (tcm *topicConfigManager) buildTopicConfigSerializeWrapper() *base.TopicConfigSerializeWrapper {
	topicConfigWrapper := base.NewTopicConfigSerializeWrapper()
	topicConfigWrapper.DataVersion = tcm.tpCfgSerialWrapper.DataVersion
	topicConfigWrapper.TpConfigTable = tcm.tpCfgSerialWrapper.TpConfigTable
	return topicConfigWrapper
}

func (tcm *topicConfigManager) load() bool {
	return tcm.cfgManagerLoader.load()
}

func (tcm *topicConfigManager) encode(prettyFormat bool) string {
	if buf, err := json.Marshal(tcm.tpCfgSerialWrapper); err == nil {
		return string(buf)
	}
	return ""
}

func (tcm *topicConfigManager) decode(content []byte) {
	if content == nil || len(content) <= 0 {
		logger.Errorf("topic config manager decode param content is nil.")
		return
	}
	if tcm == nil || tcm.tpCfgSerialWrapper == nil || tcm.tpCfgSerialWrapper.TpConfigTable == nil {
		logger.Errorf("topic config manager decode param configTable is nil.")
		return
	}

	err := json.Unmarshal(content, tcm.tpCfgSerialWrapper)
	if err != nil {
		logger.Errorf("topic config serial wrapper decode err: %s.", err)
		return
	}
}

func (tcm *topicConfigManager) configFilePath() string {
	return fmt.Sprintf("%s%c%s%ctopics.json", tcm.brokerController.storeCfg.StorePathRootDir,
		os.PathSeparator, defaultConfigDir, os.PathSeparator)
}

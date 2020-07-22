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
	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/logger"
)

// subordinateSynchronize Subordinate从Main同步信息（非消息）
// Author gaoyanlei
// Since 2017/8/10
type subordinateSynchronize struct {
	brokerController *BrokerController
	mainAddr       string
}

// newSubordinateSynchronize 初始化SubscriptionGroupManager
// Author gaoyanlei
// Since 2017/8/9
func newSubordinateSynchronize(brokerController *BrokerController) *subordinateSynchronize {
	var subordinate = new(subordinateSynchronize)
	subordinate.brokerController = brokerController
	return subordinate
}

func (subordinate *subordinateSynchronize) syncAll() {
	subordinate.syncTopicConfig()
	subordinate.syncConsumerOffset()
	subordinate.syncDelayOffset()
	subordinate.syncSubscriptionGroupConfig()
}

// syncTopicConfig 同步Topic信息
// Author rongzhihong
// Since 2017/9/18
func (subordinate *subordinateSynchronize) syncTopicConfig() {
	if subordinate.mainAddr == "" {
		return
	}

	topicWrapper := subordinate.brokerController.callOuter.GetAllTopicConfig(subordinate.mainAddr)
	if topicWrapper == nil || topicWrapper.DataVersion == nil {
		return
	}

	if topicWrapper.DataVersion != subordinate.brokerController.tpConfigManager.dataVersion {
		dataVersion := basis.DataVersion{Timestamp: topicWrapper.DataVersion.Timestamp, Counter: topicWrapper.DataVersion.Counter}
		subordinate.brokerController.tpConfigManager.dataVersion.AssignNewOne(dataVersion)
		topicConfigs := topicWrapper.TpConfigTable.TopicConfigs

		subordinate.brokerController.tpConfigManager.tpCfgSerialWrapper.TpConfigTable.ClearAndPutAll(topicConfigs)
		subordinate.brokerController.tpConfigManager.cfgManagerLoader.persist()
	}
}

// syncTopicConfig 同步消费偏移量信息
// Author rongzhihong
// Since 2017/9/18
func (subordinate *subordinateSynchronize) syncConsumerOffset() {
	if subordinate.mainAddr == "" {
		return
	}

	offsetWrapper := subordinate.brokerController.callOuter.GetAllConsumerOffset(subordinate.mainAddr)
	if offsetWrapper == nil || offsetWrapper.OffsetTable == nil {
		return
	}

	subordinate.brokerController.csmOffsetManager.offsets.PutAll(offsetWrapper.OffsetTable)
	subordinate.brokerController.csmOffsetManager.cfgManagerLoader.persist()
	buf, _ := common.Encode(offsetWrapper)
	logger.Infof("update subordinate consumer offset from main. mainAddr=%s, offsetTable=%s.", subordinate.mainAddr, string(buf))
}

// syncTopicConfig 同步定时偏移量信息
// Author rongzhihong
// Since 2017/9/18
func (subordinate *subordinateSynchronize) syncDelayOffset() {
	if subordinate.mainAddr == "" {
		return
	}

	delayOffset := subordinate.brokerController.callOuter.GetAllDelayOffset(subordinate.mainAddr)
	if delayOffset == "" {
		logger.Infof("update subordinate delay offset from main, but delayOffset is empty. mainAddr=%s.", subordinate.mainAddr)
		return
	}

	filePath := common.GetDelayOffsetStorePath(subordinate.brokerController.storeCfg.StorePathRootDir)
	common.String2File([]byte(delayOffset), filePath)
	logger.Infof("update subordinate delay offset from main. mainAddr=%s, delayOffset=%s.", subordinate.mainAddr, delayOffset)
}

// syncTopicConfig 同步订阅信息
// Author rongzhihong
// Since 2017/9/18
func (subordinate *subordinateSynchronize) syncSubscriptionGroupConfig() {
	if subordinate.mainAddr == "" {
		return
	}
	subscriptionWrapper := subordinate.brokerController.callOuter.GetAllSubscriptionGroupConfig(subordinate.mainAddr)
	if subscriptionWrapper == nil {
		return
	}

	subordinateDataVersion := subordinate.brokerController.subGroupManager.subTable.DataVersion
	if subordinateDataVersion.Timestamp != subscriptionWrapper.DataVersion.Timestamp ||
		subordinateDataVersion.Counter != subscriptionWrapper.DataVersion.Counter {
		dataVersion := basis.DataVersion{Timestamp: subscriptionWrapper.DataVersion.Timestamp, Counter: subscriptionWrapper.DataVersion.Counter}
		subscriptionGroupManager := subordinate.brokerController.subGroupManager
		subscriptionGroupManager.subTable.DataVersion.AssignNewOne(dataVersion)
		subscriptionGroupManager.subTable.ClearAndPutAll(subscriptionWrapper.SubscriptionGroupTable)
		subscriptionGroupManager.cfgManagerLoader.persist()

		buf := subscriptionGroupManager.encode(false)
		logger.Infof("sync subscription group config --> %s", string(buf))
		logger.Infof("update subordinate subscription group from main, %s.", subordinate.mainAddr)
	}
}

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

// slaveSynchronize Slave从Master同步信息（非消息）
// Author gaoyanlei
// Since 2017/8/10
type slaveSynchronize struct {
	brokerController *BrokerController
	masterAddr       string
}

// newSlaveSynchronize 初始化SubscriptionGroupManager
// Author gaoyanlei
// Since 2017/8/9
func newSlaveSynchronize(brokerController *BrokerController) *slaveSynchronize {
	var slave = new(slaveSynchronize)
	slave.brokerController = brokerController
	return slave
}

func (slave *slaveSynchronize) syncAll() {
	slave.syncTopicConfig()
	slave.syncConsumerOffset()
	slave.syncDelayOffset()
	slave.syncSubscriptionGroupConfig()
}

// syncTopicConfig 同步Topic信息
// Author rongzhihong
// Since 2017/9/18
func (slave *slaveSynchronize) syncTopicConfig() {
	if slave.masterAddr == "" {
		return
	}

	topicWrapper := slave.brokerController.callOuter.GetAllTopicConfig(slave.masterAddr)
	if topicWrapper == nil || topicWrapper.DataVersion == nil {
		return
	}

	if topicWrapper.DataVersion != slave.brokerController.tpConfigManager.dataVersion {
		dataVersion := basis.DataVersion{Timestamp: topicWrapper.DataVersion.Timestamp, Counter: topicWrapper.DataVersion.Counter}
		slave.brokerController.tpConfigManager.dataVersion.AssignNewOne(dataVersion)
		topicConfigs := topicWrapper.TpConfigTable.TopicConfigs

		slave.brokerController.tpConfigManager.tpCfgSerialWrapper.TpConfigTable.ClearAndPutAll(topicConfigs)
		slave.brokerController.tpConfigManager.cfgManagerLoader.persist()
	}
}

// syncTopicConfig 同步消费偏移量信息
// Author rongzhihong
// Since 2017/9/18
func (slave *slaveSynchronize) syncConsumerOffset() {
	if slave.masterAddr == "" {
		return
	}

	offsetWrapper := slave.brokerController.callOuter.GetAllConsumerOffset(slave.masterAddr)
	if offsetWrapper == nil || offsetWrapper.OffsetTable == nil {
		return
	}

	slave.brokerController.csmOffsetManager.offsets.PutAll(offsetWrapper.OffsetTable)
	slave.brokerController.csmOffsetManager.cfgManagerLoader.persist()
	buf, _ := common.Encode(offsetWrapper)
	logger.Infof("update slave consumer offset from master. masterAddr=%s, offsetTable=%s.", slave.masterAddr, string(buf))
}

// syncTopicConfig 同步定时偏移量信息
// Author rongzhihong
// Since 2017/9/18
func (slave *slaveSynchronize) syncDelayOffset() {
	if slave.masterAddr == "" {
		return
	}

	delayOffset := slave.brokerController.callOuter.GetAllDelayOffset(slave.masterAddr)
	if delayOffset == "" {
		logger.Infof("update slave delay offset from master, but delayOffset is empty. masterAddr=%s.", slave.masterAddr)
		return
	}

	filePath := common.GetDelayOffsetStorePath(slave.brokerController.storeCfg.StorePathRootDir)
	common.String2File([]byte(delayOffset), filePath)
	logger.Infof("update slave delay offset from master. masterAddr=%s, delayOffset=%s.", slave.masterAddr, delayOffset)
}

// syncTopicConfig 同步订阅信息
// Author rongzhihong
// Since 2017/9/18
func (slave *slaveSynchronize) syncSubscriptionGroupConfig() {
	if slave.masterAddr == "" {
		return
	}
	subscriptionWrapper := slave.brokerController.callOuter.GetAllSubscriptionGroupConfig(slave.masterAddr)
	if subscriptionWrapper == nil {
		return
	}

	slaveDataVersion := slave.brokerController.subGroupManager.subTable.DataVersion
	if slaveDataVersion.Timestamp != subscriptionWrapper.DataVersion.Timestamp ||
		slaveDataVersion.Counter != subscriptionWrapper.DataVersion.Counter {
		dataVersion := basis.DataVersion{Timestamp: subscriptionWrapper.DataVersion.Timestamp, Counter: subscriptionWrapper.DataVersion.Counter}
		subscriptionGroupManager := slave.brokerController.subGroupManager
		subscriptionGroupManager.subTable.DataVersion.AssignNewOne(dataVersion)
		subscriptionGroupManager.subTable.ClearAndPutAll(subscriptionWrapper.SubscriptionGroupTable)
		subscriptionGroupManager.cfgManagerLoader.persist()

		buf := subscriptionGroupManager.encode(false)
		logger.Infof("sync subscription group config --> %s", string(buf))
		logger.Infof("update slave subscription group from master, %s.", slave.masterAddr)
	}
}

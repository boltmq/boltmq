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
	"fmt"
	"os"

	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/protocol/subscription"
	"github.com/pquerna/ffjson/ffjson"
)

// subscriptionGroupManager 用来管理订阅组，包括订阅权限等
// Author gaoyanlei
// Since 2017/8/9
type subscriptionGroupManager struct {
	brokerController *BrokerController
	subTable         *subscription.SubscriptionGroupTable
	cfgManagerLoader *configManagerLoader
}

// newSubscriptionGroupManager 创建subscriptionGroupManager
// Author gaoyanlei
// Since 2017/8/9
func newSubscriptionGroupManager(brokerController *BrokerController) *subscriptionGroupManager {
	var sgm = new(subscriptionGroupManager)
	sgm.subTable = subscription.NewSubscriptionGroupTable()
	sgm.brokerController = brokerController
	sgm.cfgManagerLoader = newConfigManagerLoader(sgm)
	sgm.init()
	return sgm
}

// init 初始化subscriptionGroupManager
// Author gaoyanlei
// Since 2017/8/9
func (sgm *subscriptionGroupManager) init() {
	{
		subscriptionGroupConfig := subscription.NewSubscriptionGroupConfig()
		subscriptionGroupConfig.GroupName = basis.TOOLS_CONSUMER_GROUP
		sgm.subTable.Put(basis.TOOLS_CONSUMER_GROUP, subscriptionGroupConfig)
	}

	{
		subscriptionGroupConfig := subscription.NewSubscriptionGroupConfig()
		subscriptionGroupConfig.GroupName = basis.FILTERSRV_CONSUMER_GROUP
		sgm.subTable.Put(basis.FILTERSRV_CONSUMER_GROUP, subscriptionGroupConfig)
	}

	{
		subscriptionGroupConfig := subscription.NewSubscriptionGroupConfig()
		subscriptionGroupConfig.GroupName = basis.SELF_TEST_CONSUMER_GROUP
		sgm.subTable.Put(basis.SELF_TEST_CONSUMER_GROUP, subscriptionGroupConfig)
	}
}

// findSubscriptionGroupConfig 查找订阅关系
// Author gaoyanlei
// Since 2017/8/17
func (sgm *subscriptionGroupManager) findSubscriptionGroupConfig(group string) *subscription.SubscriptionGroupConfig {
	subscriptionGroupConfig := sgm.subTable.Get(group)
	if subscriptionGroupConfig != nil || !sgm.brokerController.cfg.Broker.AutoCreateSubscriptionGroup {
		return subscriptionGroupConfig
	}

	subscriptionGroupConfig = subscription.NewSubscriptionGroupConfig()
	subscriptionGroupConfig.GroupName = group
	sgm.subTable.Put(group, subscriptionGroupConfig)
	sgm.subTable.DataVersion.NextVersion()
	sgm.cfgManagerLoader.persist()
	return subscriptionGroupConfig
}

func (sgm *subscriptionGroupManager) load() bool {
	return sgm.cfgManagerLoader.load()
}

func (sgm *subscriptionGroupManager) encode(prettyFormat bool) string {
	if buf, err := ffjson.Marshal(sgm.subTable); err == nil {
		return string(buf)
	}
	return ""
}

func (sgm *subscriptionGroupManager) decode(buf []byte) {
	if buf == nil || len(buf) == 0 {
		return
	}
	if err := ffjson.Unmarshal(buf, sgm.subTable); err != nil {
		logger.Errorf("subscriptionGroupManager.Decode() err: %s, buf = %s.", err, string(buf))
	}
}

func (sgm *subscriptionGroupManager) configFilePath() string {
	return fmt.Sprintf("%s%c%s%csubscriptionGroup.json", sgm.brokerController.storeCfg.StorePathRootDir,
		os.PathSeparator, defaultConfigDir, os.PathSeparator)
}

// updateSubscriptionGroupConfig 更新订阅组配置
// Author rongzhihong
// Since 2017/9/18
func (sgm *subscriptionGroupManager) updateSubscriptionGroupConfig(config *subscription.SubscriptionGroupConfig) {
	old := sgm.subTable.Put(config.GroupName, config)
	if old != nil {
		logger.Infof("update subscription group config, old: %s, new: %s.", old, config)
	} else {
		logger.Infof("create new subscription group:%s.", config)
	}

	sgm.subTable.DataVersion.NextVersion()
	sgm.cfgManagerLoader.persist()
}

// deleteSubscriptionGroupConfig 删除某个订阅组的配置
// Author rongzhihong
// Since 2017/9/18
func (sgm *subscriptionGroupManager) deleteSubscriptionGroupConfig(groupName string) {
	old := sgm.subTable.Remove(groupName)
	if old == nil {
		logger.Warnf("delete subscription group failed, subscription group not exist. %s", old)
		return
	}

	logger.Infof("delete subscription group OK, subscription group: %s.", old)
	sgm.subTable.DataVersion.NextVersion()
	sgm.cfgManagerLoader.persist()
}

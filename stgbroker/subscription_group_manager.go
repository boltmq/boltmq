package stgbroker

import (
	"encoding/json"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/subscription"
	"github.com/pquerna/ffjson/ffjson"
)

// SubscriptionGroupManager  用来管理订阅组，包括订阅权限等
// Author gaoyanlei
// Since 2017/8/9
type SubscriptionGroupManager struct {
	BrokerController       *BrokerController
	SubscriptionGroupTable *subscription.SubscriptionGroupTable
	ConfigManagerExt       *ConfigManagerExt
}

// NewSubscriptionGroupManager 创建SubscriptionGroupManager
// Author gaoyanlei
// Since 2017/8/9
func NewSubscriptionGroupManager(brokerController *BrokerController) *SubscriptionGroupManager {
	var subscriptionGroupManager = new(SubscriptionGroupManager)
	subscriptionGroupManager.SubscriptionGroupTable = subscription.NewSubscriptionGroupTable()
	subscriptionGroupManager.BrokerController = brokerController
	subscriptionGroupManager.ConfigManagerExt = NewConfigManagerExt(subscriptionGroupManager)
	subscriptionGroupManager.init()
	return subscriptionGroupManager
}

// init 初始化SubscriptionGroupManager
// Author gaoyanlei
// Since 2017/8/9
func (subscriptionGroupManager *SubscriptionGroupManager) init() {

	{
		subscriptionGroupConfig := subscription.NewSubscriptionGroupConfig()
		subscriptionGroupConfig.GroupName = stgcommon.TOOLS_CONSUMER_GROUP
		subscriptionGroupManager.SubscriptionGroupTable.Put(stgcommon.TOOLS_CONSUMER_GROUP, subscriptionGroupConfig)
	}

	{
		subscriptionGroupConfig := subscription.NewSubscriptionGroupConfig()
		subscriptionGroupConfig.GroupName = stgcommon.FILTERSRV_CONSUMER_GROUP
		subscriptionGroupManager.SubscriptionGroupTable.Put(stgcommon.FILTERSRV_CONSUMER_GROUP, subscriptionGroupConfig)
	}

	{
		subscriptionGroupConfig := subscription.NewSubscriptionGroupConfig()
		subscriptionGroupConfig.GroupName = stgcommon.SELF_TEST_CONSUMER_GROUP
		subscriptionGroupManager.SubscriptionGroupTable.Put(stgcommon.SELF_TEST_CONSUMER_GROUP, subscriptionGroupConfig)
	}
}

// findSubscriptionGroupConfig 查找订阅关系
// Author gaoyanlei
// Since 2017/8/17
func (sgm *SubscriptionGroupManager) FindSubscriptionGroupConfig(group string) *subscription.SubscriptionGroupConfig {
	subscriptionGroupConfig := sgm.SubscriptionGroupTable.Get(group)
	if subscriptionGroupConfig == nil {
		if sgm.BrokerController.BrokerConfig.AutoCreateSubscriptionGroup {
			subscriptionGroupConfig := subscription.NewSubscriptionGroupConfig()
			subscriptionGroupConfig.GroupName = group
			sgm.SubscriptionGroupTable.Put(group, subscriptionGroupConfig)
			sgm.SubscriptionGroupTable.DataVersion.NextVersion()
			sgm.ConfigManagerExt.Persist()
			return subscriptionGroupConfig
		}
	}
	return subscriptionGroupConfig
}

func (sgm *SubscriptionGroupManager) Load() bool {

	return sgm.ConfigManagerExt.Load()
}

func (sgm *SubscriptionGroupManager) Encode(prettyFormat bool) string {
	if str, err := ffjson.Marshal(sgm.SubscriptionGroupTable); err == nil {
		return string(str)
	}
	return ""
}

func (sgm *SubscriptionGroupManager) Decode(jsonString []byte) {
	if len(jsonString) > 0 {
		json.Unmarshal(jsonString, sgm.SubscriptionGroupTable)
	}
}

func (sgm *SubscriptionGroupManager) ConfigFilePath() string {
	return GetSubscriptionGroupPath(stgcommon.GetUserHomeDir())
}

// UpdateSubscriptionGroupConfig 更新订阅组配置
// Author rongzhihong
// Since 2017/9/18
func (sgm *SubscriptionGroupManager) UpdateSubscriptionGroupConfig(config *subscription.SubscriptionGroupConfig) {
	old := sgm.SubscriptionGroupTable.Put(config.GroupName, config)
	if old != nil {
		logger.Infof("update subscription group config, old: %v, new: %v", old, config)
	} else {
		logger.Infof("create new subscription group:%v", config)
	}

	sgm.SubscriptionGroupTable.DataVersion.NextVersion()

	sgm.ConfigManagerExt.Persist()
}

// deleteSubscriptionGroupConfig 删除某个订阅组的配置
// Author rongzhihong
// Since 2017/9/18
func (sgm *SubscriptionGroupManager) DeleteSubscriptionGroupConfig(groupName string) {
	old := sgm.SubscriptionGroupTable.Remove(groupName)
	if old != nil {
		logger.Infof("delete subscription group OK, subscription group: %v", old)
		sgm.SubscriptionGroupTable.DataVersion.NextVersion()
		sgm.ConfigManagerExt.Persist()
	} else {
		logger.Warnf("delete subscription group failed, subscription group: %v not exist", old)
	}
}

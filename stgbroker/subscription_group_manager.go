package stgbroker

import (
	"encoding/json"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/subscription"
	"github.com/pquerna/ffjson/ffjson"
)

// SubscriptionGroupManager 用来管理订阅组，包括订阅权限等
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

// FindSubscriptionGroupConfig 查找订阅关系
// Author gaoyanlei
// Since 2017/8/17
func (self *SubscriptionGroupManager) FindSubscriptionGroupConfig(group string) *subscription.SubscriptionGroupConfig {
	subscriptionGroupConfig := self.SubscriptionGroupTable.Get(group)
	if subscriptionGroupConfig != nil || !self.BrokerController.BrokerConfig.AutoCreateSubscriptionGroup {
		return subscriptionGroupConfig
	}

	subscriptionGroupConfig = subscription.NewSubscriptionGroupConfig()
	subscriptionGroupConfig.GroupName = group
	self.SubscriptionGroupTable.Put(group, subscriptionGroupConfig)
	self.SubscriptionGroupTable.DataVersion.NextVersion()
	self.ConfigManagerExt.Persist()
	return subscriptionGroupConfig
}

func (self *SubscriptionGroupManager) Load() bool {
	return self.ConfigManagerExt.Load()
}

func (self *SubscriptionGroupManager) Encode(prettyFormat bool) string {
	if buf, err := ffjson.Marshal(self.SubscriptionGroupTable); err == nil {
		return string(buf)
	}
	return ""
}

func (self *SubscriptionGroupManager) Decode(buf []byte) {
	if buf == nil || len(buf) == 0 {
		return
	}
	if err := json.Unmarshal(buf, self.SubscriptionGroupTable); err != nil {
		logger.Errorf("SubscriptionGroupManager.Decode() err: %s, buf = %s", err.Error(), string(buf))
	}
}

func (self *SubscriptionGroupManager) ConfigFilePath() string {
	homeDir := stgcommon.GetUserHomeDir()
	if self.BrokerController.BrokerConfig.StorePathRootDir != "" {
		homeDir = self.BrokerController.BrokerConfig.StorePathRootDir
	}
	return GetSubscriptionGroupPath(homeDir)
}

// UpdateSubscriptionGroupConfig 更新订阅组配置
// Author rongzhihong
// Since 2017/9/18
func (self *SubscriptionGroupManager) UpdateSubscriptionGroupConfig(config *subscription.SubscriptionGroupConfig) {
	old := self.SubscriptionGroupTable.Put(config.GroupName, config)
	if old != nil {
		logger.Infof("update subscription group config, old: %s, new: %s", old.ToString(), config.ToString())
	} else {
		logger.Infof("create new subscription group:%s", config.ToString())
	}

	self.SubscriptionGroupTable.DataVersion.NextVersion()
	self.ConfigManagerExt.Persist()
}

// deleteSubscriptionGroupConfig 删除某个订阅组的配置
// Author rongzhihong
// Since 2017/9/18
func (self *SubscriptionGroupManager) DeleteSubscriptionGroupConfig(groupName string) {
	old := self.SubscriptionGroupTable.Remove(groupName)
	if old == nil {
		logger.Warnf("delete subscription group failed, subscription group not exist. %s", old.ToString())
		return
	}

	logger.Infof("delete subscription group OK, subscription group: %s", old.ToString())
	self.SubscriptionGroupTable.DataVersion.NextVersion()
	self.ConfigManagerExt.Persist()
}

package stgbroker

import (
	"encoding/json"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/subscription"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"os/user"
)

// SubscriptionGroupManager  用来管理订阅组，包括订阅权限等
// Author gaoyanlei
// Since 2017/8/9
type SubscriptionGroupManager struct {
	SubscriptionGroupTable *sync.Map

	BrokerController *BrokerController

	SubscriptionGroupConfigs []subscription.SubscriptionGroupConfig `json:"subscriptionGroupTable"`

	DataVersion stgcommon.DataVersion `json:"dataVersion"`

	configManagerExt *ConfigManagerExt
	// TODO  Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
}

// NewSubscriptionGroupManager 初始化SubscriptionGroupManager
// Author gaoyanlei
// Since 2017/8/9
func NewSubscriptionGroupManager(brokerController *BrokerController) *SubscriptionGroupManager {
	var subscriptionGroupManager = new(SubscriptionGroupManager)
	subscriptionGroupManager.SubscriptionGroupTable = sync.NewMap()
	subscriptionGroupManager.BrokerController = brokerController
	subscriptionGroupManager.configManagerExt = NewConfigManagerExt(subscriptionGroupManager)
	subscriptionGroupManager.init()
	return subscriptionGroupManager
}

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
func (self *SubscriptionGroupManager) findSubscriptionGroupConfig(group string) *subscription.SubscriptionGroupConfig {
	subscriptionGroupConfig, _ := self.SubscriptionGroupTable.Get(group)
	if value, ok := subscriptionGroupConfig.(*subscription.SubscriptionGroupConfig); ok {
		if value == nil {
			subscriptionGroupConfig := subscription.NewSubscriptionGroupConfig()
			subscriptionGroupConfig.GroupName = group
			self.SubscriptionGroupTable.Put(group, subscriptionGroupConfig)
			self.DataVersion.NextVersion()
			// TODO  this.persist();
		}
		return value
	}
	return nil
}

func (self *SubscriptionGroupManager) Load() bool {

	return self.configManagerExt.Load()
}

func (self *SubscriptionGroupManager) Encode(prettyFormat bool) string {
	return ""
}

func (self *SubscriptionGroupManager) Decode(jsonString []byte) {
	if len(jsonString) > 0 {
		subscriptionGroupManagernew := new(SubscriptionGroupManager)
		json.Unmarshal(jsonString, subscriptionGroupManagernew)
		for _, v := range subscriptionGroupManagernew.SubscriptionGroupConfigs {
			if b, err := json.Marshal(v); err == nil {
				self.SubscriptionGroupTable.Put(v.GroupName, string(b))
			}
		}
	}
}

func (self *SubscriptionGroupManager) ConfigFilePath() string {
	user, _ := user.Current()
	return GetSubscriptionGroupPath(user.HomeDir)
}

package stgbroker

import (
	"encoding/json"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/subscription"
	"github.com/pquerna/ffjson/ffjson"
	"os/user"
)

// SubscriptionGroupManager  用来管理订阅组，包括订阅权限等
// Author gaoyanlei
// Since 2017/8/9
type SubscriptionGroupManager struct {
	BrokerController *BrokerController

	SubscriptionGroupTable *subscription.SubscriptionGroupTable
	DataVersion            stgcommon.DataVersion `json:"dataVersion"`

	configManagerExt *ConfigManagerExt
}

// NewSubscriptionGroupManager 初始化SubscriptionGroupManager
// Author gaoyanlei
// Since 2017/8/9
func NewSubscriptionGroupManager(brokerController *BrokerController) *SubscriptionGroupManager {
	var subscriptionGroupManager = new(SubscriptionGroupManager)
	subscriptionGroupManager.SubscriptionGroupTable = subscription.NewSubscriptionGroupTable()
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
func (sgm *SubscriptionGroupManager) findSubscriptionGroupConfig(group string) *subscription.SubscriptionGroupConfig {
	subscriptionGroupConfig:= sgm.SubscriptionGroupTable.Get(group)
	if subscriptionGroupConfig == nil {
		if sgm.BrokerController.BrokerConfig.AutoCreateSubscriptionGroup {
			subscriptionGroupConfig := subscription.NewSubscriptionGroupConfig()
			subscriptionGroupConfig.GroupName = group
			sgm.SubscriptionGroupTable.Put(group, subscriptionGroupConfig)
			sgm.DataVersion.NextVersion()
			sgm.configManagerExt.Persist()
			return subscriptionGroupConfig
		}
	}
	return subscriptionGroupConfig
}

func (sgm *SubscriptionGroupManager) Load() bool {

	return sgm.configManagerExt.Load()
}

func (sgm *SubscriptionGroupManager) Encode(prettyFormat bool) string {
	if b, err := ffjson.Marshal(sgm.SubscriptionGroupTable); err == nil {
		fmt.Println("SubscriptionGroupManager" + string(b) + "leng" + string(sgm.SubscriptionGroupTable.Size()))
		fmt.Println(sgm.SubscriptionGroupTable.Size())
		return string(b)
	}
	return ""
}

func (sgm *SubscriptionGroupManager) Decode(jsonString []byte) {
	if len(jsonString) > 0 {
		json.Unmarshal(jsonString, sgm.SubscriptionGroupTable)
	}
}

func (sgm *SubscriptionGroupManager) ConfigFilePath() string {
	user, _ := user.Current()
	return GetSubscriptionGroupPath(user.HomeDir)
}

package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/subscription"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
)

// SubscriptionGroupManager  用来管理订阅组，包括订阅权限等
// Author gaoyanlei
// Since 2017/8/9
type SubscriptionGroupManager struct {
	SubscriptionGroupTable *sync.Map

	BrokerController *BrokerController

	// TODO  DataVersion dataVersion = new DataVersion();
	// TODO  Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
}

// NewSubscriptionGroupManager 初始化SubscriptionGroupManager
// Author gaoyanlei
// Since 2017/8/9
func NewSubscriptionGroupManager(brokerController *BrokerController) *SubscriptionGroupManager {
	var subscriptionGroupManager = new(SubscriptionGroupManager)
	subscriptionGroupManager.SubscriptionGroupTable = sync.NewMap()
	subscriptionGroupManager.BrokerController = brokerController
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



func (self *SubscriptionGroupManager) Load() bool{

	return true
}

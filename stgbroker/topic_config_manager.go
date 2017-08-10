package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"github.com/deckarep/golang-set"
)

type TopicConfigManager struct {
	// TODO Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
	LockTimeoutMillis int64
	// TODO Lock lockTopicConfigTable = new ReentrantLock();
	BrokerController *BrokerController
	TopicConfigTable *sync.Map
	// TODO DataVersion dataVersion = new DataVersion();
	SystemTopicList mapset.Set
}

// NewTopicConfigManager 初始化SubscriptionGroupManager
// @author gaoyanlei
// @since 2017/8/9
func NewTopicConfigManager(brokerController *BrokerController) *TopicConfigManager {
	var topicConfigManager = new(TopicConfigManager)
	topicConfigManager.BrokerController = brokerController
	topicConfigManager.init()
	return topicConfigManager
}

func (topicConfigManager *TopicConfigManager) init() {

	// SELF_TEST_TOPIC
	{
		topicName := stgcommon.SELF_TEST_TOPIC
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		topicConfigManager.SystemTopicList = mapset.NewSet()
		topicConfigManager.SystemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1
		topicConfig.WriteQueueNums = 1
		topicConfigManager.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// DEFAULT_TOPIC
	{
		if topicConfigManager.BrokerController.BrokerConfig.AutoCreateTopicEnable {
			topicName := stgcommon.DEFAULT_TOPIC
			topicConfig := stgcommon.NewTopicConfigByName(topicName)
			topicConfigManager.SystemTopicList = mapset.NewSet()
			topicConfigManager.SystemTopicList.Add(topicConfig)
			topicConfig.ReadQueueNums = topicConfigManager.BrokerController.BrokerConfig.DefaultTopicQueueNums
			topicConfig.WriteQueueNums = 1
			topicConfig.Perm = constant.PERM_INHERIT | constant.PERM_READ | constant.PERM_WRITE
			topicConfigManager.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
		}

	}

	// BENCHMARK_TOPIC
	{
		topicName := stgcommon.BENCHMARK_TOPIC
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		topicConfigManager.SystemTopicList = mapset.NewSet()
		topicConfigManager.SystemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1024
		topicConfig.WriteQueueNums = 1024
		topicConfigManager.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	//集群名字
	{
		topicName := topicConfigManager.BrokerController.BrokerConfig.BrokerClusterName
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		topicConfigManager.SystemTopicList = mapset.NewSet()
		topicConfigManager.SystemTopicList.Add(topicConfig)
		perm := constant.PERM_INHERIT
		if topicConfigManager.BrokerController.BrokerConfig.ClusterTopicEnable {
			perm |= constant.PERM_READ | constant.PERM_WRITE
		}
		topicConfig.Perm = int32(perm)
		topicConfigManager.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// 服务器名字
	{
		topicName := topicConfigManager.BrokerController.BrokerConfig.BrokerName
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		topicConfigManager.SystemTopicList = mapset.NewSet()
		topicConfigManager.SystemTopicList.Add(topicConfig)
		perm := constant.PERM_INHERIT
		if topicConfigManager.BrokerController.BrokerConfig.BrokerTopicEnable {
			perm |= constant.PERM_READ | constant.PERM_WRITE
		}
		topicConfig.Perm = int32(perm)
		topicConfig.WriteQueueNums = 1
		topicConfig.ReadQueueNums = 1
		topicConfigManager.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// SELF_TEST_TOPIC
	{
		topicName := stgcommon.OFFSET_MOVED_EVENT
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		topicConfigManager.SystemTopicList = mapset.NewSet()
		topicConfigManager.SystemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1
		topicConfig.WriteQueueNums = 1
		topicConfigManager.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}
}

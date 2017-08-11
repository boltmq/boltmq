package stgbroker

import (
	"errors"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"github.com/deckarep/golang-set"
	lock "sync"
)

type TopicConfigManager struct {
	// TODO Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
	LockTimeoutMillis    int64
	lockTopicConfigTable lock.Mutex
	BrokerController     *BrokerController
	TopicConfigTable     *sync.Map
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

func (topicConfigManager *TopicConfigManager) isSystemTopic(topic string) bool {
	return topicConfigManager.SystemTopicList.Contains(topic)
}

func (topicConfigManager *TopicConfigManager) getSystemTopic(topic string) mapset.Set {
	return topicConfigManager.SystemTopicList
}

func (topicConfigManager *TopicConfigManager) isTopicCanSendMessage(topic string) bool {
	if topic == stgcommon.DEFAULT_TOPIC || topic == topicConfigManager.BrokerController.BrokerConfig.BrokerClusterName {
		return false
	}
	return true
}

func (topicConfigManager *TopicConfigManager) selectTopicConfig(topic string) *stgcommon.TopicConfig {

	topicConfig, err := topicConfigManager.TopicConfigTable.Get(topic)
	if err != nil {
		return nil
	}

	if value, ok := topicConfig.(*stgcommon.TopicConfig); ok {
		return value
	}

	return nil
}

// createTopicInSendMessageMethod 创建topic
// @author gaoyanlei
// @since 2017/8/10
func (topicConfigManager *TopicConfigManager) createTopicInSendMessageMethod(topic, defaultTopic,
	remoteAddress string, clientDefaultTopicQueueNums, topicSysFlag int32) (topicConfig *stgcommon.TopicConfig, err error) {
	topicConfigManager.lockTopicConfigTable.Lock()
	defer topicConfigManager.lockTopicConfigTable.Lock()
	tc, err := topicConfigManager.TopicConfigTable.Get(topic)
	// 是否新创建topic
	createNew := false

	// 如果获取到topic并且没有出错则反会topicConig
	if err == nil && tc != nil {
		if value, ok := tc.(*stgcommon.TopicConfig); ok {
			return value, nil
		}

	}
	autoCreateTopicEnable := topicConfigManager.BrokerController.BrokerConfig.AutoCreateTopicEnable

	// 如果通过topic获取不到topic或者服务器不允许自动创建 则直接返回
	if tc == nil && !autoCreateTopicEnable {
		return nil, errors.New("No permissions to create topic")
	}

	// 如果没有获取到topic，服务允许自动创建
	value, err := topicConfigManager.TopicConfigTable.Get(defaultTopic)
	if value != nil && err == nil {
		if defaultTopicConfig, ok := value.(*stgcommon.TopicConfig); ok {
			if constant.IsInherited(defaultTopicConfig.Perm) {

				// 设置queueNums个数
				var queueNums int32
				if clientDefaultTopicQueueNums > defaultTopicConfig.WriteQueueNums {
					queueNums = defaultTopicConfig.WriteQueueNums
				} else {
					queueNums = clientDefaultTopicQueueNums
				}
				if queueNums < 0 {
					queueNums = 0
				}
				perm := defaultTopicConfig.Perm
				perm &= 0xFFFFFFFF ^ constant.PERM_INHERIT
				topicConfig.WriteQueueNums = queueNums
				topicConfig.ReadQueueNums = queueNums
				topicConfig.TopicSysFlag = topicSysFlag
				topicConfig.TopicFilterType = defaultTopicConfig.TopicFilterType

			} else {
				return nil, errors.New("No permissions to create topic")
			}
		} else {
			return nil, errors.New("reate new topic failed, because the default topic not exit")
		}
	}

	if topicConfig != nil {
		topicConfigManager.TopicConfigTable.Put(topic, topicConfig)
		// TODO this.dataVersion.nextVersion();
		createNew = true
		// TODO this.persist();
	}

	// 如果为新建则向所有Broker注册
	if createNew {
		// TODO  this.brokerController.registerBrokerAll(false, true);
	}
	return
}

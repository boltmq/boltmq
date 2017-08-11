package stgbroker

import (
	"errors"
	"fmt"
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
	var self = new(TopicConfigManager)
	self.BrokerController = brokerController
	self.init()
	return self
}

func (self *TopicConfigManager) init() {

	// SELF_TEST_TOPIC
	{
		topicName := stgcommon.SELF_TEST_TOPIC
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		self.SystemTopicList = mapset.NewSet()
		self.SystemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1
		topicConfig.WriteQueueNums = 1
		self.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// DEFAULT_TOPIC
	{
		if self.BrokerController.BrokerConfig.AutoCreateTopicEnable {
			topicName := stgcommon.DEFAULT_TOPIC
			topicConfig := stgcommon.NewTopicConfigByName(topicName)
			self.SystemTopicList = mapset.NewSet()
			self.SystemTopicList.Add(topicConfig)
			topicConfig.ReadQueueNums = self.BrokerController.BrokerConfig.DefaultTopicQueueNums
			topicConfig.WriteQueueNums = 1
			topicConfig.Perm = constant.PERM_INHERIT | constant.PERM_READ | constant.PERM_WRITE
			self.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
		}

	}

	// BENCHMARK_TOPIC
	{
		topicName := stgcommon.BENCHMARK_TOPIC
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		self.SystemTopicList = mapset.NewSet()
		self.SystemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1024
		topicConfig.WriteQueueNums = 1024
		self.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	//集群名字
	{
		topicName := self.BrokerController.BrokerConfig.BrokerClusterName
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		self.SystemTopicList = mapset.NewSet()
		self.SystemTopicList.Add(topicConfig)
		perm := constant.PERM_INHERIT
		if self.BrokerController.BrokerConfig.ClusterTopicEnable {
			perm |= constant.PERM_READ | constant.PERM_WRITE
		}
		topicConfig.Perm = int32(perm)
		self.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// 服务器名字
	{
		topicName := self.BrokerController.BrokerConfig.BrokerName
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		self.SystemTopicList = mapset.NewSet()
		self.SystemTopicList.Add(topicConfig)
		perm := constant.PERM_INHERIT
		if self.BrokerController.BrokerConfig.BrokerTopicEnable {
			perm |= constant.PERM_READ | constant.PERM_WRITE
		}
		topicConfig.Perm = int32(perm)
		topicConfig.WriteQueueNums = 1
		topicConfig.ReadQueueNums = 1
		self.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// SELF_TEST_TOPIC
	{
		topicName := stgcommon.OFFSET_MOVED_EVENT
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		self.SystemTopicList = mapset.NewSet()
		self.SystemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1
		topicConfig.WriteQueueNums = 1
		self.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}
}

func (self *TopicConfigManager) isSystemTopic(topic string) bool {
	return self.SystemTopicList.Contains(topic)
}

func (self *TopicConfigManager) getSystemTopic(topic string) mapset.Set {
	return self.SystemTopicList
}

func (self *TopicConfigManager) isTopicCanSendMessage(topic string) bool {
	if topic == stgcommon.DEFAULT_TOPIC || topic == self.BrokerController.BrokerConfig.BrokerClusterName {
		return false
	}
	return true
}

func (self *TopicConfigManager) selectTopicConfig(topic string) *stgcommon.TopicConfig {

	topicConfig, err := self.TopicConfigTable.Get(topic)
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
func (self *TopicConfigManager) createTopicInSendMessageMethod(topic, defaultTopic,
	remoteAddress string, clientDefaultTopicQueueNums, topicSysFlag int32) (topicConfig *stgcommon.TopicConfig, err error) {
	self.lockTopicConfigTable.Lock()
	defer self.lockTopicConfigTable.Lock()
	tc, err := self.TopicConfigTable.Get(topic)
	// 是否新创建topic
	createNew := false

	// 如果获取到topic并且没有出错则反会topicConig
	if err == nil && tc != nil {
		if value, ok := tc.(*stgcommon.TopicConfig); ok {
			return value, nil
		}

	}
	autoCreateTopicEnable := self.BrokerController.BrokerConfig.AutoCreateTopicEnable

	// 如果通过topic获取不到topic或者服务器不允许自动创建 则直接返回
	if tc == nil && !autoCreateTopicEnable {
		return nil, errors.New("No permissions to create topic")
	}

	// 如果没有获取到topic，服务允许自动创建
	value, err := self.TopicConfigTable.Get(defaultTopic)
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
		self.TopicConfigTable.Put(topic, topicConfig)
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

// updateTopicConfig 更新topic信息
// @author gaoyanlei
// @since 2017/8/10
func (self *TopicConfigManager) updateTopicConfig(topicConfig *stgcommon.TopicConfig, err error) {
	value, err := self.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	if value != nil && err == nil {
		fmt.Sprint("update topic config, old:%s,new:%s", value, topicConfig)
	}
	fmt.Sprint("create new topic :%s", topicConfig)
	// TODO this.dataVersion.nextVersion();
	// TODO this.persist();
}

func updateOrderTopicConfig( /**  KVTable orderKVTableFromNs */ ) {

}

// isOrderTopic 判断是否是顺序topic
// @author gaoyanlei
// @since 2017/8/10
func (self *TopicConfigManager) isOrderTopic(topic string) bool {
	value, err := self.TopicConfigTable.Get(topic)
	if value == nil && err != nil {
		return false
	} else {
		if topicConfig, ok := value.(*stgcommon.TopicConfig); ok {
			return topicConfig.Order
		}
	}
	return false
}

// deleteTopicConfig 删除topic
// @author gaoyanlei
// @since 2017/8/10
func (self *TopicConfigManager) deleteTopicConfig(topic string) {
	value, err := self.TopicConfigTable.Remove(topic)
	if value != nil && err == nil {
		fmt.Sprintf("delete topic config OK")
		// TODO this.dataVersion.nextVersion()
		// TODO 	this.persist()
	} else {
		fmt.Sprintf("delete topic config failed, topic: %s not exist", topic)
	}

}

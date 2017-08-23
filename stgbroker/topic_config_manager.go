package stgbroker

import (
	"encoding/json"
	"errors"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"github.com/deckarep/golang-set"
	"os/user"
	lock "sync"
)

type TopicConfigManager struct {
	// TODO Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
	LockTimeoutMillis    int64
	lockTopicConfigTable lock.Mutex
	BrokerController     *BrokerController
	TopicConfigTable     *sync.Map
	DataVersion          *stgcommon.DataVersion
	SystemTopicList      mapset.Set
	configManagerExt     *ConfigManagerExt
}

// NewTopicConfigManager 初始化TopicConfigManager
// Author gaoyanlei
// Since 2017/8/9
func NewTopicConfigManager(brokerController *BrokerController) *TopicConfigManager {
	var topicConfigManager = new(TopicConfigManager)
	topicConfigManager.BrokerController = brokerController
	topicConfigManager.DataVersion = stgcommon.NewDataVersion()
	topicConfigManager.TopicConfigTable = sync.NewMap()
	topicConfigManager.init()
	topicConfigManager.configManagerExt = NewConfigManagerExt(topicConfigManager)
	return topicConfigManager
}

func (tcm *TopicConfigManager) init() {

	// SELF_TEST_TOPIC
	{
		topicName := stgcommon.SELF_TEST_TOPIC
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		tcm.SystemTopicList = mapset.NewSet()
		tcm.SystemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1
		topicConfig.WriteQueueNums = 1
		tcm.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// DEFAULT_TOPIC
	{
		if tcm.BrokerController.BrokerConfig.AutoCreateTopicEnable {
			topicName := stgcommon.DEFAULT_TOPIC
			topicConfig := stgcommon.NewTopicConfigByName(topicName)
			tcm.SystemTopicList = mapset.NewSet()
			tcm.SystemTopicList.Add(topicConfig)
			topicConfig.ReadQueueNums = tcm.BrokerController.BrokerConfig.DefaultTopicQueueNums
			topicConfig.WriteQueueNums = 1
			topicConfig.Perm = constant.PERM_INHERIT | constant.PERM_READ | constant.PERM_WRITE
			tcm.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
		}

	}

	// BENCHMARK_TOPIC
	{
		topicName := stgcommon.BENCHMARK_TOPIC
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		tcm.SystemTopicList = mapset.NewSet()
		tcm.SystemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1024
		topicConfig.WriteQueueNums = 1024
		tcm.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	//集群名字
	{
		topicName := tcm.BrokerController.BrokerConfig.BrokerClusterName
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		tcm.SystemTopicList = mapset.NewSet()
		tcm.SystemTopicList.Add(topicConfig)
		perm := constant.PERM_INHERIT
		if tcm.BrokerController.BrokerConfig.ClusterTopicEnable {
			perm |= constant.PERM_READ | constant.PERM_WRITE
		}
		topicConfig.Perm = perm
		tcm.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// 服务器名字
	{
		topicName := tcm.BrokerController.BrokerConfig.BrokerName
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		tcm.SystemTopicList = mapset.NewSet()
		tcm.SystemTopicList.Add(topicConfig)
		perm := constant.PERM_INHERIT
		if tcm.BrokerController.BrokerConfig.BrokerTopicEnable {
			perm |= constant.PERM_READ | constant.PERM_WRITE
		}
		topicConfig.Perm = perm
		topicConfig.WriteQueueNums = 1
		topicConfig.ReadQueueNums = 1
		tcm.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// SELF_TEST_TOPIC
	{
		topicName := stgcommon.OFFSET_MOVED_EVENT
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		tcm.SystemTopicList = mapset.NewSet()
		tcm.SystemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1
		topicConfig.WriteQueueNums = 1
		tcm.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}
}

func (tcm *TopicConfigManager) isSystemTopic(topic string) bool {
	return tcm.SystemTopicList.Contains(topic)
}

func (tcm *TopicConfigManager) isTopicCanSendMessage(topic string) bool {
	if topic == stgcommon.DEFAULT_TOPIC || topic == tcm.BrokerController.BrokerConfig.BrokerClusterName {
		return false
	}
	return true
}

// selectTopicConfig 根据topic查找
// Author gaoyanlei
// Since 2017/8/11
func (tcm *TopicConfigManager) selectTopicConfig(topic string) *stgcommon.TopicConfig {

	topicConfig, err := tcm.TopicConfigTable.Get(topic)
	if err != nil {
		return nil
	}

	if value, ok := topicConfig.(*stgcommon.TopicConfig); ok {
		return value
	}

	return nil
}

// createTopicInSendMessageMethod 创建topic
// Author gaoyanlei
// Since 2017/8/10
func (tcm *TopicConfigManager) createTopicInSendMessageMethod(topic, defaultTopic,
	remoteAddress string, clientDefaultTopicQueueNums, topicSysFlag int) (topicConfig *stgcommon.TopicConfig, err error) {
	tcm.lockTopicConfigTable.Lock()
	defer tcm.lockTopicConfigTable.Lock()
	tc, err := tcm.TopicConfigTable.Get(topic)
	// 是否新创建topic
	createNew := false

	// 如果获取到topic并且没有出错则反会topicConig
	if err == nil && tc != nil {
		if value, ok := tc.(*stgcommon.TopicConfig); ok {
			return value, nil
		}

	}
	autoCreateTopicEnable := tcm.BrokerController.BrokerConfig.AutoCreateTopicEnable

	// 如果通过topic获取不到topic或者服务器不允许自动创建 则直接返回
	if tc == nil && !autoCreateTopicEnable {
		return nil, errors.New("No permissions to create topic")
	}

	// 如果没有获取到topic，服务允许自动创建
	value, err := tcm.TopicConfigTable.Get(defaultTopic)
	if value != nil && err == nil {
		if defaultTopicConfig, ok := value.(*stgcommon.TopicConfig); ok {
			if constant.IsInherited(defaultTopicConfig.Perm) {
				// 设置queueNums个数
				var queueNums int
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
		tcm.TopicConfigTable.Put(topic, topicConfig)
		tcm.DataVersion.NextVersion()
		createNew = true
		// TODO this.persist();
	}

	// 如果为新建则向所有Broker注册
	if createNew {
		// TODO  this.brokerController.registerBrokerAll(false, true);
	}
	return
}

// createTopicInSendMessageBackMethod 该方法没有判断broker权限.
// Author gaoyanlei
// Since 2017/8/11
func (tcm *TopicConfigManager) createTopicInSendMessageBackMethod(topic string, perm,
	clientDefaultTopicQueueNums, topicSysFlag int) (topicConfig *stgcommon.TopicConfig, err error) {
	tcm.lockTopicConfigTable.Lock()
	defer tcm.lockTopicConfigTable.Lock()
	tc, err := tcm.TopicConfigTable.Get(topic)
	// 是否新创建topic
	createNew := false

	// 如果获取到topic并且没有出错则反会topicConig
	if err == nil && tc != nil {
		if topicConfig, ok := tc.(*stgcommon.TopicConfig); ok {
			return topicConfig, nil
		}

	}
	topicConfig.WriteQueueNums = clientDefaultTopicQueueNums
	topicConfig.ReadQueueNums = clientDefaultTopicQueueNums
	topicConfig.TopicSysFlag = topicSysFlag
	topicConfig.Perm = perm

	tcm.TopicConfigTable.Put(topic, topicConfig)
	tcm.DataVersion.NextVersion()
	createNew = true
	tcm.configManagerExt.Persist()

	// 如果为新建则向所有Broker注册
	if createNew {
		// TODO  this.brokerController.registerBrokerAll(false, true);
	}
	return
}

// updateTopicConfig 更新topic信息
// Author gaoyanlei
// Since 2017/8/10
func (tcm *TopicConfigManager) updateTopicConfig(topicConfig *stgcommon.TopicConfig, err error) {
	value, err := tcm.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	if value != nil && err == nil {
		fmt.Sprint("update topic config, old:%s,new:%s", value, topicConfig)
	}
	fmt.Sprint("create new topic :%s", topicConfig)
	tcm.DataVersion.NextVersion()
	// TODO this.persist();
}

// updateOrderTopicConfig 更新顺序topic
// Author gaoyanlei
// Since 2017/8/11
func (tcm *TopicConfigManager) updateOrderTopicConfig(orderKVTableFromNs body.KVTable) {
	if orderKVTableFromNs.Table != nil {
		isChange := false
		orderTopics := mapset.NewSet()
		for k, _ := range orderKVTableFromNs.Table {
			orderTopics.Add(k)
		}
		// set遍历
		for val := range orderTopics.Iter() {
			value, _ := tcm.TopicConfigTable.Get(val)
			if topicConfig, ok := value.(*stgcommon.TopicConfig); ok {
				topicConfig.Order = true
				isChange = true
			}
		}

		for it := tcm.TopicConfigTable.Iterator(); it.HasNext(); {
			topic, _, _ := it.Next()
			if !orderTopics.Contains(topic) {
				value, _ := tcm.TopicConfigTable.Get(topic)
				if topicConfig, ok := value.(*stgcommon.TopicConfig); ok {
					topicConfig.Order = true
					isChange = true
				}
			}
		}

		if isChange {
			tcm.DataVersion.NextVersion()
			// TODO  this.persist();
		}
	}
}

// isOrderTopic 判断是否是顺序topic
// Author gaoyanlei
// Since 2017/8/10
func (tcm *TopicConfigManager) IsOrderTopic(topic string) bool {
	value, err := tcm.TopicConfigTable.Get(topic)
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
// Author gaoyanlei
// Since 2017/8/10
func (tcm *TopicConfigManager) deleteTopicConfig(topic string) {
	value, err := tcm.TopicConfigTable.Remove(topic)
	if value != nil && err == nil {
		fmt.Sprintf("delete topic config OK")
		tcm.DataVersion.NextVersion()
		// TODO 	this.persist()
	} else {
		fmt.Sprintf("delete topic config failed, topic: %s not exist", topic)
	}
}

// buildTopicConfigSerializeWrapper 创建TopicConfigSerializeWrapper
// Author gaoyanlei
// Since 2017/8/11
func (tcm *TopicConfigManager) buildTopicConfigSerializeWrapper() {
	topicConfigSerializeWrapper := body.NewTopicConfigSerializeWrapper()
	topicConfigSerializeWrapper.DataVersion = tcm.DataVersion
	topicConfigSerializeWrapper.TopicConfigTable = tcm.TopicConfigTable
}

func (tcm *TopicConfigManager) Load() bool {
	return tcm.configManagerExt.Load()
}

func (tcm *TopicConfigManager) Encode(prettyFormat bool) string {
	topicConfigSerializeWrapper := body.NewTopicConfigSerializeWrapper()
	topicConfigSerializeWrapper.TopicConfigTable = tcm.TopicConfigTable
	topicConfigSerializeWrapper.DataVersion = tcm.DataVersion
	if b, err := json.Marshal(topicConfigSerializeWrapper); err == nil {
		return string(b)
	}
	return ""
}

func (tcm *TopicConfigManager) Decode(jsonString []byte) {
	if len(jsonString) > 0 {
		topicConfigSerializeWrapper := body.NewTopicConfigSerializeWrapper()
		json.Unmarshal(jsonString, topicConfigSerializeWrapper)
		for _, v := range topicConfigSerializeWrapper.TopicConfigs {
			if b, err := json.Marshal(v); err == nil {
				tcm.TopicConfigTable.Put(v.TopicName, b)
			}
		}
	}
}

func (tcm *TopicConfigManager) ConfigFilePath() string {
	user, _ := user.Current()
	return GetTopicConfigPath(user.HomeDir)
}

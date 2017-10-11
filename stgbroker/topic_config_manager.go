package stgbroker

import (
	"encoding/json"
	"errors"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	set "github.com/deckarep/golang-set"
	"sync"
)

type TopicConfigManager struct {
	LockTimeoutMillis           int64
	lockTopicConfigTable        sync.Mutex
	BrokerController            *BrokerController
	TopicConfigSerializeWrapper *body.TopicConfigSerializeWrapper
	SystemTopicList             set.Set
	ConfigManagerExt            *ConfigManagerExt
	DataVersion                 *stgcommon.DataVersion
}

// NewTopicConfigManager 初始化TopicConfigManager
// Author gaoyanlei
// Since 2017/8/9
func NewTopicConfigManager(brokerController *BrokerController) *TopicConfigManager {
	var topicConfigManager = new(TopicConfigManager)
	topicConfigManager.BrokerController = brokerController
	topicConfigManager.TopicConfigSerializeWrapper = body.NewTopicConfigSerializeWrapper()
	topicConfigManager.init()
	topicConfigManager.ConfigManagerExt = NewConfigManagerExt(topicConfigManager)
	topicConfigManager.DataVersion = stgcommon.NewDataVersion()
	return topicConfigManager
}

func (self *TopicConfigManager) init() {

	// SELF_TEST_TOPIC
	{
		topicName := stgcommon.SELF_TEST_TOPIC
		topicConfig := stgcommon.NewTopicConfig(topicName)
		self.SystemTopicList = set.NewSet()
		self.SystemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1
		topicConfig.WriteQueueNums = 1
		//logger.Infof("topicConfigManager init: %s", topicConfig.ToString())
		self.TopicConfigSerializeWrapper.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// DEFAULT_TOPIC
	{
		autoCreateTopicEnable := self.BrokerController.BrokerConfig.AutoCreateTopicEnable
		logger.Infof("self.BrokerController.BrokerConfig.AutoCreateTopicEnable=%t", autoCreateTopicEnable)
		if autoCreateTopicEnable {
			topicName := stgcommon.DEFAULT_TOPIC
			topicConfig := stgcommon.NewTopicConfig(topicName)
			self.SystemTopicList = set.NewSet()
			self.SystemTopicList.Add(topicConfig)
			topicConfig.ReadQueueNums = self.BrokerController.BrokerConfig.DefaultTopicQueueNums
			topicConfig.WriteQueueNums = self.BrokerController.BrokerConfig.DefaultTopicQueueNums
			topicConfig.Perm = constant.PERM_INHERIT | constant.PERM_READ | constant.PERM_WRITE
			//logger.Infof("topicConfigManager init: %s", topicConfig.ToString())
			self.TopicConfigSerializeWrapper.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
		}
	}

	// BENCHMARK_TOPIC
	{
		topicName := stgcommon.BENCHMARK_TOPIC
		topicConfig := stgcommon.NewTopicConfig(topicName)
		self.SystemTopicList = set.NewSet()
		self.SystemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1024
		topicConfig.WriteQueueNums = 1024
		//logger.Infof("topicConfigManager init: %s", topicConfig.ToString())
		self.TopicConfigSerializeWrapper.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// DefaultCluster
	{
		topicName := self.BrokerController.BrokerConfig.BrokerClusterName
		topicConfig := stgcommon.NewTopicConfig(topicName)
		self.SystemTopicList = set.NewSet()
		self.SystemTopicList.Add(topicConfig)
		perm := constant.PERM_INHERIT
		if self.BrokerController.BrokerConfig.ClusterTopicEnable {
			perm |= constant.PERM_READ | constant.PERM_WRITE
		}
		topicConfig.Perm = perm
		//logger.Infof("topicConfigManager init: %s", topicConfig.ToString())
		self.TopicConfigSerializeWrapper.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// DEFAULT_BROKER
	{
		topicName := self.BrokerController.BrokerConfig.BrokerName
		topicConfig := stgcommon.NewTopicConfig(topicName)
		self.SystemTopicList = set.NewSet()
		self.SystemTopicList.Add(topicConfig)
		perm := constant.PERM_INHERIT
		if self.BrokerController.BrokerConfig.BrokerTopicEnable {
			perm |= constant.PERM_READ | constant.PERM_WRITE
		}
		topicConfig.Perm = perm
		topicConfig.WriteQueueNums = 1
		topicConfig.ReadQueueNums = 1
		//logger.Infof("topicConfigManager init: %s", topicConfig.ToString())
		self.TopicConfigSerializeWrapper.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// SELF_TEST_TOPIC
	{
		topicName := stgcommon.OFFSET_MOVED_EVENT
		topicConfig := stgcommon.NewTopicConfig(topicName)
		self.SystemTopicList = set.NewSet()
		self.SystemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1
		topicConfig.WriteQueueNums = 1
		//logger.Infof("topicConfigManager init: %s", topicConfig.ToString())
		self.TopicConfigSerializeWrapper.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
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
func (tcm *TopicConfigManager) SelectTopicConfig(topic string) *stgcommon.TopicConfig {
	topicConfig := tcm.TopicConfigSerializeWrapper.TopicConfigTable.Get(topic)
	if topicConfig != nil {
		return topicConfig
	}
	return nil
}

// createTopicInSendMessageMethod 创建topic
// Author gaoyanlei
// Since 2017/8/10
func (tcm *TopicConfigManager) CreateTopicInSendMessageMethod(topic, defaultTopic,
	remoteAddress string, clientDefaultTopicQueueNums int32, topicSysFlag int) (topicConfig *stgcommon.TopicConfig, err error) {
	tcm.lockTopicConfigTable.Lock()
	defer tcm.lockTopicConfigTable.Unlock()
	tc := tcm.TopicConfigSerializeWrapper.TopicConfigTable.Get(topic)
	// 是否新创建topic
	createNew := false

	// 如果获取到topic并且没有出错则反会topicConig
	if tc == nil {
		return tc, nil
	}

	autoCreateTopicEnable := tcm.BrokerController.BrokerConfig.AutoCreateTopicEnable

	// 如果通过topic获取不到topic或者服务器不允许自动创建 则直接返回
	if tc == nil && !autoCreateTopicEnable {
		return nil, errors.New("No permissions to create topic")
	}

	// 如果没有获取到topic，服务允许自动创建
	defaultTopicConfig := tcm.TopicConfigSerializeWrapper.TopicConfigTable.Get(defaultTopic)
	if defaultTopicConfig != nil {
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
			topicConfig = &stgcommon.TopicConfig{
				TopicName:       topic,
				WriteQueueNums:  queueNums,
				ReadQueueNums:   queueNums,
				TopicSysFlag:    topicSysFlag,
				TopicFilterType: defaultTopicConfig.TopicFilterType,
			}
		} else {
			return nil, errors.New("No permissions to create topic")
		}
	} else {
		return nil, errors.New("reate new topic failed, because the default topic not exit")
	}

	if topicConfig != nil {
		tcm.TopicConfigSerializeWrapper.TopicConfigTable.Put(topic, topicConfig)
		tcm.TopicConfigSerializeWrapper.DataVersion.NextVersion()
		createNew = true
		tcm.ConfigManagerExt.Persist()
	}

	// 如果为新建则向所有Broker注册
	if createNew {
		tcm.BrokerController.RegisterBrokerAll(false, true)
	}
	return
}

// createTopicInSendMessageBackMethod 该方法没有判断broker权限.
// Author gaoyanlei
// Since 2017/8/11
func (tcm *TopicConfigManager) CreateTopicInSendMessageBackMethod(topic string, clientDefaultTopicQueueNums int32, perm, topicSysFlag int) (topicConfig *stgcommon.TopicConfig, err error) {
	tc := tcm.TopicConfigSerializeWrapper.TopicConfigTable.Get(topic)
	// 是否新创建topic
	createNew := false

	// 如果获取到topic并且没有出错则反会topicConig
	if tc != nil {
		return tc, nil
	}
	topicConfig = &stgcommon.TopicConfig{
		TopicName:      topic,
		WriteQueueNums: clientDefaultTopicQueueNums,
		ReadQueueNums:  clientDefaultTopicQueueNums,
		TopicSysFlag:   topicSysFlag,
		Perm:           perm,
	}
	tcm.TopicConfigSerializeWrapper.TopicConfigTable.Put(topic, topicConfig)
	tcm.TopicConfigSerializeWrapper.DataVersion.NextVersion()
	createNew = true
	tcm.ConfigManagerExt.Persist()

	// 如果为新建则向所有Broker注册
	if createNew {
		tcm.BrokerController.RegisterBrokerAll(false, true)
	}
	return
}

// updateTopicConfig 更新topic信息
// Author gaoyanlei
// Since 2017/8/10
func (tcm *TopicConfigManager) UpdateTopicConfig(topicConfig *stgcommon.TopicConfig) {
	old := tcm.TopicConfigSerializeWrapper.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	if old != nil {
		logger.Infof("update topic config, old:%s, new:%s", old.ToString(), topicConfig.ToString())
	}
	logger.Infof("create new topic: %s", topicConfig.ToString())
	tcm.TopicConfigSerializeWrapper.DataVersion.NextVersion()
	tcm.ConfigManagerExt.Persist()
}

// updateOrderTopicConfig 更新顺序topic
// Author gaoyanlei
// Since 2017/8/11
func (self *TopicConfigManager) updateOrderTopicConfig(orderKVTable *body.KVTable) {
	if orderKVTable == nil || orderKVTable.Table == nil {
		logger.Info("orderKVTable or orderKVTable.Table is nil")
		return
	}

	isChange := false
	orderTopics := set.NewSet()
	// 通过set集合去重
	for k, _ := range orderKVTable.Table {
		orderTopics.Add(k)
	}

	// set遍历
	for val := range orderTopics.Iter() {
		if value, ok := val.(string); ok {
			topicConfig := self.TopicConfigSerializeWrapper.TopicConfigTable.Get(value)
			if topicConfig != nil {
				topicConfig.Order = true
				isChange = true
			} else {
				// todo: 打印日志？
			}
		}
	}
	self.TopicConfigSerializeWrapper.TopicConfigTable.Foreach(func(topic string, topicConfig *stgcommon.TopicConfig) {
		if topicConfig != nil && !orderTopics.Contains(topicConfig) {
			topicConfig.Order = true
			isChange = true
		}
	})

	if isChange {
		self.TopicConfigSerializeWrapper.DataVersion.NextVersion()
		self.ConfigManagerExt.Persist()
	}
}

// isOrderTopic 判断是否是顺序topic
// Author gaoyanlei
// Since 2017/8/10
func (tcm *TopicConfigManager) IsOrderTopic(topic string) bool {
	topicConfig := tcm.TopicConfigSerializeWrapper.TopicConfigTable.Get(topic)
	if topicConfig == nil {
		return false
	}
	return topicConfig.Order
}

// deleteTopicConfig 删除topic
// Author gaoyanlei
// Since 2017/8/10
func (tcm *TopicConfigManager) DeleteTopicConfig(topic string) {
	value := tcm.TopicConfigSerializeWrapper.TopicConfigTable.Remove(topic)
	if value != nil {
		logger.Info("delete topic config OK")
		tcm.TopicConfigSerializeWrapper.DataVersion.NextVersion()
		tcm.ConfigManagerExt.Persist()
	} else {
		logger.Infof("delete topic config failed, topic: %s not exist", topic)
	}
}

// buildTopicConfigSerializeWrapper 创建TopicConfigSerializeWrapper
// Author gaoyanlei
// Since 2017/8/11
func (self *TopicConfigManager) buildTopicConfigSerializeWrapper() *body.TopicConfigSerializeWrapper {
	topicConfigWrapper := body.NewTopicConfigSerializeWrapper()
	topicConfigWrapper.DataVersion = self.TopicConfigSerializeWrapper.DataVersion
	topicConfigWrapper.TopicConfigTable = self.TopicConfigSerializeWrapper.TopicConfigTable
	return topicConfigWrapper
}

func (tcm *TopicConfigManager) Load() bool {
	return tcm.ConfigManagerExt.Load()
}

func (self *TopicConfigManager) Encode(prettyFormat bool) string {
	if buf, err := json.Marshal(self.TopicConfigSerializeWrapper); err == nil {
		return string(buf)
	}
	return ""
}

func (self *TopicConfigManager) Decode(content []byte) {
	if content == nil || len(content) <= 0 {
		logger.Errorf("TopicConfigManager.Decode() param content is nil")
		return
	}
	if self == nil || self.TopicConfigSerializeWrapper == nil || self.TopicConfigSerializeWrapper.TopicConfigTable == nil {
		logger.Errorf("TopicConfigManager.TopicConfigTable is nil")
		return
	}

	err := json.Unmarshal(content, self.TopicConfigSerializeWrapper)
	if err != nil {
		logger.Errorf("TopicConfigSerializeWrapper.Decode() err: %s", err.Error())
		return
	}
}

func (self *TopicConfigManager) ConfigFilePath() string {
	return GetTopicConfigPath(stgcommon.GetUserHomeDir())
}

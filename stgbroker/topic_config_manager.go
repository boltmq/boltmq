package stgbroker

import (
	"encoding/json"
	"errors"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"github.com/deckarep/golang-set"
	"os/user"
	lock "sync"
)

type TopicConfigManager struct {
	LockTimeoutMillis           int64
	lockTopicConfigTable        lock.Mutex
	BrokerController            *BrokerController
	TopicConfigSerializeWrapper *body.TopicConfigSerializeWrapper
	SystemTopicList             mapset.Set
	configManagerExt            *ConfigManagerExt
}

// NewTopicConfigManager 初始化TopicConfigManager
// Author gaoyanlei
// Since 2017/8/9
func NewTopicConfigManager(brokerController *BrokerController) *TopicConfigManager {
	var topicConfigManager = new(TopicConfigManager)
	topicConfigManager.BrokerController = brokerController
	topicConfigManager.TopicConfigSerializeWrapper = body.NewTopicConfigSerializeWrapper()
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
		tcm.TopicConfigSerializeWrapper.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// DEFAULT_TOPIC
	{
		if tcm.BrokerController.BrokerConfig.AutoCreateTopicEnable {
			topicName := stgcommon.DEFAULT_TOPIC
			topicConfig := stgcommon.NewTopicConfigByName(topicName)
			tcm.SystemTopicList = mapset.NewSet()
			tcm.SystemTopicList.Add(topicConfig)
			topicConfig.ReadQueueNums = tcm.BrokerController.BrokerConfig.DefaultTopicQueueNums
			topicConfig.WriteQueueNums = tcm.BrokerController.BrokerConfig.DefaultTopicQueueNums
			topicConfig.Perm = constant.PERM_INHERIT | constant.PERM_READ | constant.PERM_WRITE
			tcm.TopicConfigSerializeWrapper.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
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
		tcm.TopicConfigSerializeWrapper.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
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
		tcm.TopicConfigSerializeWrapper.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
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
		tcm.TopicConfigSerializeWrapper.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
	}

	// SELF_TEST_TOPIC
	{
		topicName := stgcommon.OFFSET_MOVED_EVENT
		topicConfig := stgcommon.NewTopicConfigByName(topicName)
		tcm.SystemTopicList = mapset.NewSet()
		tcm.SystemTopicList.Add(topicConfig)
		topicConfig.ReadQueueNums = 1
		topicConfig.WriteQueueNums = 1
		tcm.TopicConfigSerializeWrapper.TopicConfigTable.Put(topicConfig.TopicName, topicConfig)
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

	topicConfig := tcm.TopicConfigSerializeWrapper.TopicConfigTable.Get(topic)
	if topicConfig != nil {
		return topicConfig
	}
	return nil
}

// createTopicInSendMessageMethod 创建topic
// Author gaoyanlei
// Since 2017/8/10
func (tcm *TopicConfigManager) createTopicInSendMessageMethod(topic, defaultTopic,
	remoteAddress string, clientDefaultTopicQueueNums int32, topicSysFlag int) (topicConfig *stgcommon.TopicConfig, err error) {
	tcm.lockTopicConfigTable.Lock()
	defer tcm.lockTopicConfigTable.Unlock()
	tc := tcm.TopicConfigSerializeWrapper.TopicConfigTable.Get(topic)
	// 是否新创建topic
	createNew := false

	// 如果获取到topic并且没有出错则反会topicConig
	if tc == nil && tc != nil {
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
		tcm.configManagerExt.Persist()
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
func (tcm *TopicConfigManager) createTopicInSendMessageBackMethod(topic string,
	clientDefaultTopicQueueNums int32, perm, topicSysFlag int) (topicConfig *stgcommon.TopicConfig, err error) {
	tcm.lockTopicConfigTable.Lock()
	defer tcm.lockTopicConfigTable.Lock()
	tc := tcm.TopicConfigSerializeWrapper.TopicConfigTable.Get(topic)
	// 是否新创建topic
	createNew := false

	// 如果获取到topic并且没有出错则反会topicConig
	if tc != nil {
		return tc, nil
	}

	topicConfig.WriteQueueNums = clientDefaultTopicQueueNums
	topicConfig.ReadQueueNums = clientDefaultTopicQueueNums
	topicConfig.TopicSysFlag = topicSysFlag
	topicConfig.Perm = perm

	tcm.TopicConfigSerializeWrapper.TopicConfigTable.Put(topic, topicConfig)
	tcm.TopicConfigSerializeWrapper.DataVersion.NextVersion()
	createNew = true
	tcm.configManagerExt.Persist()

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
		logger.Infof("update topic config, old:%v,new:%v", old, topicConfig)
	}
	logger.Infof("create new topic :%v", topicConfig)
	tcm.TopicConfigSerializeWrapper.DataVersion.NextVersion()
	tcm.configManagerExt.Persist()
}

// updateOrderTopicConfig 更新顺序topic
// Author gaoyanlei
// Since 2017/8/11
func (tcm *TopicConfigManager) updateOrderTopicConfig(orderKVTableFromNs body.KVTable) {
	if orderKVTableFromNs.Table != nil {
		isChange := false
		orderTopics := mapset.NewSet()
		// 通过set集合去重
		for k, _ := range orderKVTableFromNs.Table {
			orderTopics.Add(k)
		}

		// set遍历
		for val := range orderTopics.Iter() {
			topicConfig := tcm.TopicConfigSerializeWrapper.TopicConfigTable.Get(val.(string))
			topicConfig.Order = true
			isChange = true
		}
		tcm.TopicConfigSerializeWrapper.TopicConfigTable.Foreach(func(k string, v *stgcommon.TopicConfig) {
			if !orderTopics.Contains(v) {
				v.Order = true
				isChange = true
			}
		})

		if isChange {
			tcm.TopicConfigSerializeWrapper.DataVersion.NextVersion()
			tcm.configManagerExt.Persist()
		}
	}
}

// isOrderTopic 判断是否是顺序topic
// Author gaoyanlei
// Since 2017/8/10
func (tcm *TopicConfigManager) IsOrderTopic(topic string) bool {
	topicConfig := tcm.TopicConfigSerializeWrapper.TopicConfigTable.Get(topic)
	if topicConfig == nil {
		return false
	} else {
		return topicConfig.Order
	}
	return false
}

// deleteTopicConfig 删除topic
// Author gaoyanlei
// Since 2017/8/10
func (tcm *TopicConfigManager) deleteTopicConfig(topic string) {
	value := tcm.TopicConfigSerializeWrapper.TopicConfigTable.Remove(topic)
	if value != nil {
		logger.Info("delete topic config OK")
		tcm.TopicConfigSerializeWrapper.DataVersion.NextVersion()
		tcm.configManagerExt.Persist()
	} else {
		logger.Infof("delete topic config failed, topic: %s not exist", topic)
	}
}

// buildTopicConfigSerializeWrapper 创建TopicConfigSerializeWrapper
// Author gaoyanlei
// Since 2017/8/11
func (tcm *TopicConfigManager) buildTopicConfigSerializeWrapper() *body.TopicConfigSerializeWrapper {
	topicConfigSerializeWrapper := body.NewTopicConfigSerializeWrapper()
	topicConfigSerializeWrapper.DataVersion = tcm.TopicConfigSerializeWrapper.DataVersion
	topicConfigSerializeWrapper.TopicConfigTable = tcm.TopicConfigSerializeWrapper.TopicConfigTable
	return topicConfigSerializeWrapper
}

func (tcm *TopicConfigManager) Load() bool {
	return tcm.configManagerExt.Load()
}

func (tcm *TopicConfigManager) Encode(prettyFormat bool) string {
	if b, err := json.Marshal(tcm.TopicConfigSerializeWrapper); err == nil {
		return string(b)
	}
	return ""
}

func (tcm *TopicConfigManager) Decode(jsonString []byte) {
	if len(jsonString) > 0 {
		json.Unmarshal(jsonString, tcm.TopicConfigSerializeWrapper)
	}
}

func (tcm *TopicConfigManager) ConfigFilePath() string {
	user, _ := user.Current()
	return GetTopicConfigPath(user.HomeDir)
}

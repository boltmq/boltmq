package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
)

// SlaveSynchronize Slave从Master同步信息（非消息）
// Author gaoyanlei
// Since 2017/8/10
type SlaveSynchronize struct {
	BrokerController *BrokerController
	masterAddr       string
}

// NewSlaveSynchronize 初始化SubscriptionGroupManager
// Author gaoyanlei
// Since 2017/8/9
func NewSlaveSynchronize(brokerController *BrokerController) *SlaveSynchronize {
	var slaveSynchronize = new(SlaveSynchronize)
	slaveSynchronize.BrokerController = brokerController
	return slaveSynchronize
}

func (self *SlaveSynchronize) syncAll() {
	self.syncConsumerOffset()
	self.syncTopicConfig()
	self.syncDelayOffset()
	self.syncSubscriptionGroupConfig()
}

// syncTopicConfig 同步Topic配置文件
// Author rongzhihong
// Since 2017/9/18
func (slave *SlaveSynchronize) syncTopicConfig() {
	if slave.masterAddr == "" {
		return
	}

	topicWrapper := slave.BrokerController.BrokerOuterAPI.GetAllTopicConfig(slave.masterAddr)
	if topicWrapper == nil || topicWrapper.DataVersion == nil {
		return
	}

	if topicWrapper.DataVersion != slave.BrokerController.TopicConfigManager.DataVersion {
		dataVersion := stgcommon.NewDataVersion(topicWrapper.DataVersion.Timestamp)
		dataVersion.Counter = topicWrapper.DataVersion.Counter

		slave.BrokerController.TopicConfigManager.DataVersion.AssignNewOne(*dataVersion)
		slave.BrokerController.TopicConfigManager.TopicConfigSerializeWrapper.TopicConfigTable.Clear()

		topicConfigs := topicWrapper.TopicConfigTable.TopicConfigs
		slave.BrokerController.TopicConfigManager.TopicConfigSerializeWrapper.TopicConfigTable.PutAll(topicConfigs)
		slave.BrokerController.TopicConfigManager.ConfigManagerExt.Persist()
	}
}

// syncTopicConfig 同步偏移量配置文件
// Author rongzhihong
// Since 2017/9/18
func (slave *SlaveSynchronize) syncConsumerOffset() {
	if slave.masterAddr == "" {
		return
	}

	offsetWrapper := slave.BrokerController.BrokerOuterAPI.GetAllConsumerOffset(slave.masterAddr)
	if offsetWrapper == nil || offsetWrapper.OffsetTable == nil {
		return
	}

	slave.BrokerController.ConsumerOffsetManager.Offsets.PutAll(offsetWrapper.OffsetTable)
	slave.BrokerController.ConsumerOffsetManager.configManagerExt.Persist()
	buf := offsetWrapper.CustomEncode(offsetWrapper)
	logger.Infof("update slave consumer offset from master. masterAddr=%s, offsetTable=%s", slave.masterAddr, string(buf))
}

// syncTopicConfig 同步定时偏移量配置文件
// Author rongzhihong
// Since 2017/9/18
func (self *SlaveSynchronize) syncDelayOffset() {
	if self.masterAddr == "" {
		return
	}

	delayOffset := self.BrokerController.BrokerOuterAPI.GetAllDelayOffset(self.masterAddr)
	if delayOffset == "" {
		logger.Infof("update slave delay offset from master, but delayOffset is empty. masterAddr=%s", self.masterAddr)
		return
	}

	fileName := config.GetDelayOffsetStorePath(self.BrokerController.MessageStoreConfig.StorePathRootDir)
	stgcommon.String2File([]byte(delayOffset), fileName)
	logger.Infof("update slave delay offset from master. masterAddr=%s, delayOffset=%s", self.masterAddr, delayOffset)
}

// syncTopicConfig 同步订阅配置文件
// Author rongzhihong
// Since 2017/9/18
func (self *SlaveSynchronize) syncSubscriptionGroupConfig() {
	if self.masterAddr == "" {
		return
	}
	subscriptionWrapper := self.BrokerController.BrokerOuterAPI.GetAllSubscriptionGroupConfig(self.masterAddr)
	if subscriptionWrapper == nil {
		return
	}

	if !subscriptionWrapper.DataVersion.Equals(&(self.BrokerController.SubscriptionGroupManager.SubscriptionGroupTable.DataVersion)) {
		dataVersion := stgcommon.NewDataVersion(subscriptionWrapper.DataVersion.Timestamp)
		dataVersion.Counter = subscriptionWrapper.DataVersion.Counter

		subscriptionGroupManager := self.BrokerController.SubscriptionGroupManager
		subscriptionGroupManager.SubscriptionGroupTable.DataVersion.AssignNewOne(*dataVersion)
		subscriptionGroupManager.SubscriptionGroupTable.Clear()
		subscriptionGroupManager.SubscriptionGroupTable.PutAll(subscriptionWrapper.SubscriptionGroupTable)
		subscriptionGroupManager.ConfigManagerExt.Persist()

		buf := subscriptionGroupManager.Encode(false)
		logger.Infof("syncSubscriptionGroupConfig --> %s", buf)
		logger.Infof("update slave subscription group from master, %s", self.masterAddr)
	}
}

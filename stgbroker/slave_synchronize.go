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

func (slaveSynchronize *SlaveSynchronize) syncAll() {
	slaveSynchronize.syncConsumerOffset()
	slaveSynchronize.syncTopicConfig()
	slaveSynchronize.syncDelayOffset()
	slaveSynchronize.syncSubscriptionGroupConfig()

}

// syncTopicConfig 同步Topic配置文件
// Author rongzhihong
// Since 2017/9/18
func (slave *SlaveSynchronize) syncTopicConfig() {
	masterAddrBak := slave.masterAddr
	if "" != (masterAddrBak) {
		topicWrapper := slave.BrokerController.BrokerOuterAPI.GetAllTopicConfig(masterAddrBak)
		if slave.BrokerController.TopicConfigManager.DataVersion != topicWrapper.DataVersion {
			slave.BrokerController.TopicConfigManager.DataVersion.AssignNewOne(
				stgcommon.DataVersion{Timestatmp: topicWrapper.DataVersion.Timestatmp, Counter: topicWrapper.DataVersion.Counter})

			slave.BrokerController.TopicConfigManager.TopicConfigSerializeWrapper.TopicConfigTable.Clear()
			slave.BrokerController.TopicConfigManager.TopicConfigSerializeWrapper.TopicConfigTable.PutAll(
				topicWrapper.TopicConfigTable.TopicConfigs,
			)
			slave.BrokerController.TopicConfigManager.ConfigManagerExt.Persist()
		}
	}
}

// syncTopicConfig 同步偏移量配置文件
// Author rongzhihong
// Since 2017/9/18
func (slave *SlaveSynchronize) syncConsumerOffset() {
	masterAddrBak := slave.masterAddr
	if "" != (masterAddrBak) {
		offsetWrapper := slave.BrokerController.BrokerOuterAPI.GetAllConsumerOffset(masterAddrBak)
		slave.BrokerController.ConsumerOffsetManager.Offsets.PutAll(offsetWrapper.OffsetTable)
		slave.BrokerController.ConsumerOffsetManager.configManagerExt.Persist()
		logger.Infof("update slave consumer offset from master, %s", masterAddrBak)
	}
}

// syncTopicConfig 同步定时偏移量配置文件
// Author rongzhihong
// Since 2017/9/18
func (slave *SlaveSynchronize) syncDelayOffset() {
	masterAddrBak := slave.masterAddr
	if "" != (masterAddrBak) {
		delayOffset := slave.BrokerController.BrokerOuterAPI.GetAllDelayOffset(masterAddrBak)
		if delayOffset != "" {
			fileName := config.GetDelayOffsetStorePath(slave.BrokerController.MessageStoreConfig.StorePathRootDir)
			stgcommon.String2File([]byte(delayOffset), fileName)
		}
		logger.Infof("update slave delay offset from master, %s", masterAddrBak)
	}
}

// syncTopicConfig 同步订阅配置文件
// Author rongzhihong
// Since 2017/9/18
func (slave *SlaveSynchronize) syncSubscriptionGroupConfig() {
	masterAddrBak := slave.masterAddr
	if "" != (masterAddrBak) {
		subscriptionWrapper := slave.BrokerController.BrokerOuterAPI.GetAllSubscriptionGroupConfig(masterAddrBak)

		if slave.BrokerController.SubscriptionGroupManager.SubscriptionGroupTable.DataVersion != subscriptionWrapper.DataVersion {
			subscriptionGroupManager := slave.BrokerController.SubscriptionGroupManager
			subscriptionGroupManager.SubscriptionGroupTable.DataVersion.AssignNewOne(
				stgcommon.DataVersion{Timestatmp: subscriptionWrapper.DataVersion.Timestatmp, Counter: subscriptionWrapper.DataVersion.Counter})
			subscriptionGroupManager.SubscriptionGroupTable.Clear()
			subscriptionGroupManager.SubscriptionGroupTable.PutAll(subscriptionWrapper.SubscriptionGroupTable)
			subscriptionGroupManager.ConfigManagerExt.Persist()

			logger.Infof("update slave Subscription Group from master, %s", masterAddrBak)
		}
	}
}

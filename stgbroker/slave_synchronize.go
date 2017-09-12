package stgbroker

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

func (slaveSynchronize *SlaveSynchronize) syncTopicConfig() {
	masterAddrBak := slaveSynchronize.masterAddr
	if "" != (masterAddrBak) {

	}
}

func (slaveSynchronize *SlaveSynchronize) syncConsumerOffset() {

}
func (slaveSynchronize *SlaveSynchronize) syncDelayOffset() {

}
func (slaveSynchronize *SlaveSynchronize) syncSubscriptionGroupConfig() {

}

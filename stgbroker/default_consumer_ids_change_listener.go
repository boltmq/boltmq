package stgbroker

import (
	"net"
)

// DefaultConsumerIdsChangeListener ConsumerId列表变化，通知所有Consumer
// Author gaoyanlei
// Since 2017/8/9
type DefaultConsumerIdsChangeListener struct {
	BrokerController *BrokerController
}

// NewDefaultConsumerIdsChangeListener 初始化
// Author gaoyanlei
// Since 2017/8/9
func NewDefaultConsumerIdsChangeListener(brokerController *BrokerController) *DefaultConsumerIdsChangeListener {
	var defaultConsumerIdsChangeListener = new(DefaultConsumerIdsChangeListener)
	defaultConsumerIdsChangeListener.BrokerController = brokerController
	return defaultConsumerIdsChangeListener
}

// ConsumerIdsChanged 通知Consumer改变
// Author gaoyanlei
// Since 2017/8/9
func (listener *DefaultConsumerIdsChangeListener) ConsumerIdsChanged(group string, channels []net.Conn) {
	if channels != nil && listener.BrokerController.BrokerConfig.NotifyConsumerIdsChangedEnable {
		for _, conn := range channels {
			listener.BrokerController.Broker2Client.notifyConsumerIdsChanged(conn, group)
		}
	}
}

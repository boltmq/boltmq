package stgbroker

import "fmt"

// DefaultConsumerIdsChangeListener ConsumerId列表变化，通知所有Consumer
// @author gaoyanlei
// @since 2017/8/9
type DefaultConsumerIdsChangeListener struct {
	BrokerController *BrokerController
}

// NewDefaultConsumerIdsChangeListener 初始化
// @author gaoyanlei
// @since 2017/8/9
func NewDefaultConsumerIdsChangeListener(brokerController *BrokerController) *DefaultConsumerIdsChangeListener {
	var defaultConsumerIdsChangeListener = new(DefaultConsumerIdsChangeListener)
	defaultConsumerIdsChangeListener.BrokerController = brokerController
	return defaultConsumerIdsChangeListener
}

// ConsumerIdsChanged 通知Consumer改变
// @author gaoyanlei
// @since 2017/8/9
func (listener *DefaultConsumerIdsChangeListener) ConsumerIdsChanged(group string, channels []string) {
	if channels != nil && listener.BrokerController.BrokerConfig.NotifyConsumerIdsChangedEnable {

		for index, value := range channels {
			// TODO Broker主动通知Consumer，Id列表发生变化，
			fmt.Printf("arr[%d]=%d \n", index, value)
		}
	}
}

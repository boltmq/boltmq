package stgbroker

// Broker2Client Broker主动调用客户端接口
// @author gaoyanlei
// @since 2017/8/9
type Broker2Client struct {
	BrokerController *BrokerController
}

// NewBroker2Clientr Broker2Client
// @author gaoyanlei
// @since 2017/8/9
func NewBroker2Clientr(brokerController *BrokerController) *Broker2Client {
	var broker2Client = new(Broker2Client)
	broker2Client.BrokerController = brokerController
	return broker2Client
}

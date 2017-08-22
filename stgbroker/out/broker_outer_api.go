package out

import "git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"

// BrokerOuterAPI Broker对外调用的API封装
// @author gaoyanlei
// @since 2017/8/9
type BrokerOuterAPI struct {
	// TODO Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
	// TODO RemotingClient remotingClient;

	topAddressing *namesrv.TopAddressing
	nameSrvAddr   string
}

// NewBrokerOuterAPI 初始化
// @author gaoyanlei
// @since 2017/8/9
func NewBrokerOuterAPI( /** NettyClientConfig nettyClientConfig */ ) *BrokerOuterAPI {
	var brokerController = new(BrokerOuterAPI)
	// TODO brokerController.remotingClient=
	return brokerController
}

func (self *BrokerOuterAPI) UpdateNameServerAddressList(addrs string) {

}

func (self *BrokerOuterAPI) FetchNameServerAddr() {

}

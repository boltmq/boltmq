package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"net"
	protocol "git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	commonprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
)

// Broker2Client Broker主动调用客户端接口
// Author gaoyanlei
// Since 2017/8/9
type Broker2Client struct {
	BrokerController *BrokerController
}

// NewBroker2Clientr Broker2Client
// Author gaoyanlei
// Since 2017/8/9
func NewBroker2Clientr(brokerController *BrokerController) *Broker2Client {
	var broker2Client = new(Broker2Client)
	broker2Client.BrokerController = brokerController
	return broker2Client
}

// notifyConsumerIdsChanged 消费Id 列表改变通知
// Author rongzhihong
// Since 2017/9/11
func (b2c *Broker2Client) notifyConsumerIdsChanged(conn net.Conn, consumerGroup string) {
	defer utils.RecoveredFn()
	if "" == consumerGroup {
		logger.Error("notifyConsumerIdsChanged consumerGroup is null")
		return
	}

	requestHeader := &header.NotifyConsumerIdsChangedRequestHeader{ConsumerGroup: consumerGroup}
	request := protocol.CreateRequestCommand(commonprotocol.NOTIFY_CONSUMER_IDS_CHANGED, requestHeader)
	b2c.BrokerController.RemotingServer.InvokeOneway(conn, request, 10)
}

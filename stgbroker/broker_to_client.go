package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	commonprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
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
func (b2c *Broker2Client) notifyConsumerIdsChanged(ctx netm.Context, consumerGroup string) {
	defer utils.RecoveredFn()
	if "" == consumerGroup {
		logger.Error("notifyConsumerIdsChanged consumerGroup is null")
		return
	}

	requestHeader := &header.NotifyConsumerIdsChangedRequestHeader{ConsumerGroup: consumerGroup}
	request := protocol.CreateRequestCommand(commonprotocol.NOTIFY_CONSUMER_IDS_CHANGED, requestHeader)
	b2c.BrokerController.RemotingServer.InvokeOneway(ctx, request, 10)
}

// CheckProducerTransactionState Broker主动回查Producer事务状态，Oneway
// Author rongzhihong
// Since 2017/9/11
func (b2c *Broker2Client) CheckProducerTransactionState(channel netm.Context, requestHeader *header.CheckTransactionStateRequestHeader,
	selectMapedBufferResult *stgstorelog.SelectMapedBufferResult) {
	request := protocol.CreateRequestCommand(commonprotocol.CHECK_TRANSACTION_STATE, requestHeader)
	request.MarkOnewayRPC()

	// TODO
	/*FileRegion fileRegion =
			new OneMessageTransfer(request.encodeHeader(selectMapedBufferResult.getSize()),
			selectMapedBufferResult);
		channel.writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
			@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			selectMapedBufferResult.release();
			if (!future.isSuccess()) {
			log.error("invokeProducer failed,", future.cause());
			}
		}
	});*/
}

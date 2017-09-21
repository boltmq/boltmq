package stgbroker

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker/client"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/mqversion"
	commonprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	body2 "git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
	"strconv"
	"strings"
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

// CallClient 调用客户端
// Author rongzhihong
// Since 2017/9/18
func (b2c *Broker2Client) CallClient(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	return b2c.BrokerController.RemotingServer.InvokeSync(ctx, request, 10000)
}

// ResetOffset 重置偏移量
// Author rongzhihong
// Since 2017/9/18
func (b2c *Broker2Client) ResetOffset(topic, group string, timeStamp int64, isForce bool) *protocol.RemotingCommand {
	response := protocol.CreateDefaultResponseCommand(nil)
	topicConfig := b2c.BrokerController.TopicConfigManager.SelectTopicConfig(topic)
	if topicConfig == nil {
		logger.Errorf("[reset-offset] reset offset failed, no topic in this broker. topic=%s", topic)
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = fmt.Sprintf("[reset-offset] reset offset failed, no topic in this broker. topic=" + topic)
		return response
	}

	offsetTable := make(map[*message.MessageQueue]int64)
	writeQueueNums, _ := strconv.Atoi(fmt.Sprintf("%s", topicConfig.WriteQueueNums))
	for i := 0; i < writeQueueNums; i++ {
		mq := &message.MessageQueue{}
		mq.BrokerName = b2c.BrokerController.BrokerConfig.BrokerName
		mq.Topic = topic
		mq.QueueId = i

		consumerOffset := b2c.BrokerController.ConsumerOffsetManager.queryOffset(group, topic, i)
		if -1 == consumerOffset {
			response.Code = protocol.SYSTEM_ERROR
			response.Remark = fmt.Sprintf("THe consumer group <%s> not exist", group)
			return response
		}

		// TODO timeStampOffset := b2c.BrokerController.MessageStore.getOffsetInQueueByTime(topic, i, timeStamp)
		timeStampOffset := int64(0)
		if isForce || timeStampOffset < consumerOffset {
			offsetTable[mq] = timeStampOffset
		} else {
			offsetTable[mq] = consumerOffset
		}
	}

	requestHeader := &header.ResetOffsetRequestHeader{}
	requestHeader.Topic = topic
	requestHeader.Group = group
	requestHeader.IsForce = isForce

	request := protocol.CreateRequestCommand(commonprotocol.RESET_CONSUMER_CLIENT_OFFSET, requestHeader)
	body := body2.NewResetOffsetBody()
	body.OffsetTable = offsetTable
	request.Body = body.Encode()

	consumerGroupInfo := b2c.BrokerController.ConsumerManager.GetConsumerGroupInfo(group)
	// Consumer在线
	if consumerGroupInfo != nil && len(consumerGroupInfo.GetAllChannel()) > 0 {
		channelInfoTable := consumerGroupInfo.ConnTable
		iterator := channelInfoTable.Iterator()
		for iterator.HasNext() {
			_, val, _ := iterator.Next()
			if channelInfo, ok := val.(*client.ChannelInfo); ok {
				version := channelInfo.Version
				if version > mqversion.V3_0_7_SNAPSHOT {
					b2c.BrokerController.RemotingServer.InvokeSync(channelInfo.Context, request, 5000)

					logger.Infof("[reset-offset] reset offset success. topic=%s, group=%s, clientId=%d",
						topic, group, channelInfo.ClientId)
				} else {

					// 如果有一个客户端是不支持该功能的，则直接返回错误，需要应用方升级。
					response.Code = protocol.SYSTEM_ERROR
					response.Remark = fmt.Sprintf("the client does not support this feature. version=%s",
						mqversion.GetVersionDesc(int(version)))

					logger.Warnf("[reset-offset] the client does not support this feature. remoteAdd=%s, version=%s",
						channelInfo.Addr, mqversion.GetVersionDesc(int(version)))

					return response
				}
			}
		}

	} else {
		// Consumer不在线
		errorInfo := fmt.Sprintf("Consumer not online, so can not reset offset, Group: %s Topic: %s Timestamp: %d",
			requestHeader.Group, requestHeader.Topic, requestHeader.Timestamp)
		logger.Error(errorInfo)
		response.Code = commonprotocol.CONSUMER_NOT_ONLINE
		response.Remark = errorInfo
		return response
	}

	response.Code = commonprotocol.SUCCESS
	resBody := body2.NewResetOffsetBody()
	resBody.OffsetTable = offsetTable
	response.Body = resBody.Encode()
	return response
}

// GetConsumeStatus Broker主动获取Consumer端的消息情况
// Author rongzhihong
// Since 2017/9/18
func (b2c *Broker2Client) GetConsumeStatus(topic, group, originClientId string) *protocol.RemotingCommand {
	response := protocol.CreateDefaultResponseCommand(nil)

	requestHeader := &header.GetConsumerStatusRequestHeader{}
	requestHeader.Topic = topic
	requestHeader.Group = group

	request := protocol.CreateRequestCommand(commonprotocol.GET_CONSUMER_STATUS_FROM_CLIENT, requestHeader)

	consumerStatusTable := make(map[string]map[*message.MessageQueue]int64)

	channelInfoTable := b2c.BrokerController.ConsumerManager.GetConsumerGroupInfo(group).ConnTable
	if nil == channelInfoTable || channelInfoTable.Size() <= 0 {
		response.Code = commonprotocol.SYSTEM_ERROR
		response.Remark = fmt.Sprintf("No Any Consumer online in the consumer group: [%s]", group)
		return response
	}

	iterator := channelInfoTable.Iterator()
	for iterator.HasNext() {
		key, value, _ := iterator.Next()
		channel, ok := key.(netm.Context)
		if !ok {
			logger.Warnf("The key=%s type is not netm.Context", key)
			continue
		}
		channelInfo, ok := value.(*client.ChannelInfo)
		if !ok {
			logger.Warnf("The value=%s type is not ChannelInfo", value)
			continue
		}

		version := channelInfo.Version
		clientId := channelInfo.ClientId
		if version < mqversion.V3_0_7_SNAPSHOT {
			// 如果有一个客户端是不支持该功能的，则直接返回错误，需要应用方升级。
			response.Code = commonprotocol.SYSTEM_ERROR
			response.Remark = fmt.Sprintf("the client does not support this feature. version=%s",
				mqversion.GetVersionDesc(int(version)))
			logger.Warnf("the client does not support this feature. version=%s",
				mqversion.GetVersionDesc(int(version)))
			return response

		} else if stgcommon.IsBlank(originClientId) || strings.EqualFold(originClientId, clientId) {
			// 不指定 originClientId 则对所有的 client 进行处理；若指定 originClientId 则只对当前
			// originClientId 进行处理
			response, err := b2c.BrokerController.RemotingServer.InvokeSync(channel, request, 5000)
			if err != nil {
				logger.Error(err)
			}
			switch response.Code {
			case commonprotocol.SUCCESS:
				if response.Body != nil && len(response.Body) > 0 {
					statusBody := &body2.GetConsumerStatusBody{}
					statusBody.Decode(response.Body)
					consumerStatusTable[clientId] = statusBody.MessageQueueTable
					logger.Infof(
						"[get-consumer-status] get consumer status success. topic=%s, group=%s, channelRemoteAddr=%s",
						topic, group, clientId)
				}
			}

			// 若指定 originClientId 相应的 client 处理完成，则退出循环
			if !stgcommon.IsBlank(originClientId) && strings.EqualFold(originClientId, clientId) {
				break
			}
		}

	}

	resBody := &body2.GetConsumerStatusBody{}
	resBody.ConsumerTable = consumerStatusTable
	response.Body = resBody.Encode()

	response.Code = commonprotocol.SUCCESS
	response.Remark = ""
	return response
}

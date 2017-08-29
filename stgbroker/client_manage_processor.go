package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/client"
	"git.oschina.net/cloudzone/smartgo/stgbroker/mqtrace"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/listener"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	//protocol2 "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"net"
)

type ClientManageProcessor struct {
	BrokerController *BrokerController
}

// NewClientManageProcessor 初始化ClientManageProcessor
// Author gaoyanlei
// Since 2017/8/9
func NewClientManageProcessor(brokerController *BrokerController) *ClientManageProcessor {
	var clientManageProcessor = new(ClientManageProcessor)
	clientManageProcessor.BrokerController = brokerController
	return clientManageProcessor
}

func (cmp *ClientManageProcessor) ProcessRequest(addr string, conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	switch request.Code {
	//case protocol2.HEART_BEAT:
	//	return cmp.heartBeat(addr, conn, request)
	//case protocol2.UNREGISTER_CLIENT:
	//	return cmp.unregisterClient(addr, conn, request)
	//case protocol2.GET_CONSUMER_LIST_BY_GROUP:
	//	return cmp.getConsumerListByGroup(addr, conn, request)
	//case protocol2.QUERY_CONSUMER_OFFSET:
	//	return cmp.queryConsumerOffset(addr, conn, request)
	//case protocol2.UPDATE_CONSUMER_OFFSET:
	//	return cmp.updateConsumerOffset(addr, conn, request)
	}
	return nil, nil
}

// heartBeat 心跳服务
// Author gaoyanlei
// Since 2017/8/23
func (cmp *ClientManageProcessor) heartBeat(addr string, conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	heartbeatData := &heartbeat.HeartbeatData{}
	heartbeatData.Decode(request.Body)
	consumerDataSet := heartbeatData.ConsumerDataSet
	channelInfo := client.NewClientChannelInfo(conn, heartbeatData.ClientID, request.Language, conn.LocalAddr().String(), request.Version)

	for value := range consumerDataSet.Iterator().C {
		if consumerData, ok := value.(*heartbeat.ConsumerData); ok {
			subscriptionGroupConfig :=
				cmp.BrokerController.SubscriptionGroupManager.findSubscriptionGroupConfig(consumerData.GroupName)
			if subscriptionGroupConfig != nil {
				topicSysFlag := 0
				if consumerData.UnitMode {
					topicSysFlag = sysflag.TopicBuildSysFlag(false, true)
				}
				newTopic := stgcommon.GetRetryTopic(consumerData.GroupName)
				cmp.BrokerController.TopicConfigManager.createTopicInSendMessageBackMethod( //
					newTopic, //
					subscriptionGroupConfig.RetryQueueNums, //
					constant.PERM_WRITE|constant.PERM_READ, topicSysFlag)
			}
			changed := cmp.BrokerController.ConsumerManager.RegisterConsumer(consumerData.GroupName, conn,
				consumerData.ConsumeType, consumerData.MessageModel, consumerData.ConsumeFromWhere, consumerData.SubscriptionDataSet)
			if changed {

			}

			// 注册Producer
			for value := range heartbeatData.ProducerDataSet.Iterator().C {
				if producerData, ok := value.(*heartbeat.ProducerData); ok {
					cmp.BrokerController.ProducerManager.RegisterProducer(producerData.GroupName, channelInfo)
				}
			}
		}
	}
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// unregisterClient 注销客户端
// Author gaoyanlei
// Since 2017/8/24
func (cmp *ClientManageProcessor) unregisterClient(addr string, conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	requestHeader := header.UnregisterClientRequestHeader{}

	channelInfo := client.NewClientChannelInfo(conn, requestHeader.ClientID, request.Language, conn.LocalAddr().String(), request.Version)

	// 注销Producer
	{
		group := requestHeader.ProducerGroup
		if group != "" {
			cmp.BrokerController.ProducerManager.UnregisterProducer(group, channelInfo)
		}
	}

	// 注销Consumer
	{
		group := requestHeader.ConsumerGroup
		if group != "" {
			cmp.BrokerController.ConsumerManager.UnregisterConsumer(group, channelInfo)
		}
	}

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

func (cmp *ClientManageProcessor) queryConsumerOffset(addr string, conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	requestHeader := &header.UpdateConsumerOffsetRequestHeader{}

	context := &mqtrace.ConsumeMessageContext{}
	context.ConsumerGroup = requestHeader.ConsumerGroup
	context.Topic = requestHeader.Topic
	context.ClientHost = conn.LocalAddr().String()
	context.Success = true
	context.Status = listener.CONSUME_SUCCESS.String()

	//
	//final SocketAddress storeHost =
	//	new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(), brokerController
	//.getNettyServerConfig().getListenPort());
	// TODO preOffset :=cmp.BrokerController.ConsumerOffsetManager.queryOffset(requestHeader.ConsumerGroup, requestHeader.Topic,requestHeader.QueueId)
	// TODO messageIds :=cmp.BrokerController.getMessageStore().getMessageIds(requestHeader.getTopic(), requestHeader.getQueueId(), preOffset, requestHeader.getCommitOffset(), storeHost);
	// TODO context.setMessageIds(messageIds);
	// TODO this.executeConsumeMessageHookAfter(context);

	cmp.BrokerController.ConsumerOffsetManager.CommitOffset(requestHeader.ConsumerGroup,
		requestHeader.Topic, requestHeader.QueueId, requestHeader.CommitOffset)
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// updateConsumerOffset 更新消费者offset
// Author gaoyanlei
// Since 2017/8/25
func (cmp *ClientManageProcessor) updateConsumerOffset(addr string, conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	requestHeader := &header.UpdateConsumerOffsetRequestHeader{}

	context := &mqtrace.ConsumeMessageContext{}
	context.ConsumerGroup = requestHeader.ConsumerGroup
	context.Topic = requestHeader.Topic
	context.ClientHost = conn.LocalAddr().String()
	context.Success = true
	context.Status = listener.CONSUME_SUCCESS.String()
	// TODO
	//final SocketAddress storeHost =
	//	new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(), brokerController
	//.getNettyServerConfig().getListenPort());

	//preOffset := cmp.BrokerController.ConsumerOffsetManager.queryOffset(requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueId)
	//// TODO messageIds
	//messageIds :=
	//	cmp.BrokerController.MessageStore.GetMessageIds(requestHeader.getTopic(),
	//		requestHeader.getQueueId(), preOffset, requestHeader.getCommitOffset(), storeHost)
	//context.setMessageIds(messageIds)
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// updateConsumerOffset 更新消费者offset
// Author gaoyanlei
// Since 2017/8/25
func (cmp *ClientManageProcessor) getConsumerListByGroup(addr string, conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	requestHeader := &header.GetConsumerListByGroupRequestHeader{}
	consumerGroupInfo := cmp.BrokerController.ConsumerManager.GetConsumerGroupInfo(requestHeader.ConsumerGroup)
	if consumerGroupInfo != nil {
		//clientIds:=consumerGroupInfo.get
	}

	// TODO
	//final SocketAddress storeHost =
	//	new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(), brokerController
	//.getNettyServerConfig().getListenPort());

	//preOffset := cmp.BrokerController.ConsumerOffsetManager.queryOffset(requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueId)
	//// TODO messageIds
	//messageIds :=
	//	cmp.BrokerController.MessageStore.GetMessageIds(requestHeader.getTopic(),
	//		requestHeader.getQueueId(), preOffset, requestHeader.getCommitOffset(), storeHost)
	//context.setMessageIds(messageIds)
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

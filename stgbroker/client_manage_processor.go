package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/client"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	protocol2 "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
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
	case protocol2.HEART_BEAT:
		return cmp.heartBeat(addr, conn, request)
	case protocol2.UNREGISTER_CLIENT:
		return cmp.heartBeat(addr, conn, request)
	case protocol2.QUERY_CONSUMER_OFFSET:
		return cmp.queryConsumerOffset(addr, conn, request)
	}
	return nil, nil
}

// heartBeat 心跳服务
// Author gaoyanlei
// Since 2017/8/23
func (cmp *ClientManageProcessor) heartBeat(addr string, conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	// TODO HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
	heartbeatData := &heartbeat.HeartbeatData{}
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
			for value := range  heartbeatData.ProducerDataSet.Iterator().C {
				if producerData, ok := value.(*heartbeat.ProducerData); ok {
					cmp.BrokerController.ProducerManager.RegisterProducer(producerData.GroupName,channelInfo)
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
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

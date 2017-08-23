package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	protocol2 "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
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
			changed := cmp.BrokerController.ConsumerManager.registerConsumer(consumerData.GroupName, conn,
				consumerData.ConsumeType, consumerData.MessageModel, consumerData.ConsumeFromWhere, consumerData.SubscriptionDataSet)
			if changed {

			}
		}
	}
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

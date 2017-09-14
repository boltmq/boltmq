package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	//protocol2 "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"net"
	"strings"
)

// AdminBrokerProcessor 管理类请求处理
// Author gaoyanlei
// Since 2017/8/23
type AdminBrokerProcessor struct {
	BrokerController *BrokerController
}

// NewAdminBrokerProcessor 初始化
// Author gaoyanlei
// Since 2017/8/23
func NewAdminBrokerProcessor(brokerController *BrokerController) *AdminBrokerProcessor {
	var adminBrokerProcessor = new(AdminBrokerProcessor)
	adminBrokerProcessor.BrokerController = brokerController
	return adminBrokerProcessor
}

func (abp *AdminBrokerProcessor) ProcessRequest(addr string, conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	//switch request.Code {
	//// 更新创建Topic
	//case protocol2.UPDATE_AND_CREATE_TOPIC:
	//	return abp.updateAndCreateTopic(addr, conn, request)
	//// 更新创建Topic
	//case protocol2.DELETE_TOPIC_IN_BROKER:
	//	return abp.deleteTopic(addr, conn, request)
	//case protocol2.GET_MAX_OFFSET:
	//	return abp.getMaxOffset(addr, conn, request)
	//}
	return nil, nil
}

func (abp *AdminBrokerProcessor) updateAndCreateTopic(addr string, conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	requestHeader := &header.CreateTopicRequestHeader{}
	if strings.EqualFold(requestHeader.Topic, abp.BrokerController.BrokerConfig.BrokerClusterName) {
		errorMsg :=
			"the topic[" + requestHeader.Topic + "] is conflict with system reserved words."
		logger.Warn(errorMsg)
		response.Code = code.SYSTEM_ERROR
		response.Remark = errorMsg
		return response, nil
	}

	topicConfig := &stgcommon.TopicConfig{
		ReadQueueNums:   requestHeader.ReadQueueNums,
		WriteQueueNums:  requestHeader.WriteQueueNums,
		TopicFilterType: requestHeader.TopicFilterType,
		Perm:            requestHeader.Perm,
	}
	if requestHeader.TopicSysFlag != 0 {
		topicConfig.TopicSysFlag = requestHeader.TopicSysFlag
	}
	abp.BrokerController.TopicConfigManager.UpdateTopicConfig(topicConfig)
	abp.BrokerController.RegisterBrokerAll(false, true)
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

func (abp *AdminBrokerProcessor) getMaxOffset(addr string, conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	responseHeader := &header.GetMaxOffsetResponseHeader{}
	//requestHeader:= &header.GetMaxOffsetRequestHeader{}
	var offset int64
	// TODO
	//abp.BrokerController.MessageStore().getMaxOffsetInQuque(requestHeader.getTopic(),
	//requestHeader.getQueueId());

	responseHeader.Offset = offset
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}
func (abp *AdminBrokerProcessor) deleteTopic(addr string, conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := &protocol.RemotingCommand{}
	responseHeader := &header.DeleteTopicRequestHeader{}
	abp.BrokerController.TopicConfigManager.deleteTopicConfig(responseHeader.Topic)
	abp.BrokerController.addDeleteTopicTask()

	logger.Infof("deleteTopic called by %v", conn.LocalAddr().String())
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

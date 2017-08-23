package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	protocol2 "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
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

func (abp *AdminBrokerProcessor) processRequest(request protocol.RemotingCommand) {
	switch request.Code {
	case protocol2.UPDATE_AND_CREATE_TOPIC:

		
	}
}

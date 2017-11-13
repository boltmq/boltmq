package stgbroker

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker/pagecache"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// QueryMessageProcessor 查询消息请求处理
// Author rongzhihong
// Since 2017/9/18
type QueryMessageProcessor struct {
	BrokerController *BrokerController
}

// NewQueryMessageProcessor 初始化QueryMessageProcessor
// Author rongzhihong
// Since 2017/9/18
func NewQueryMessageProcessor(brokerController *BrokerController) *QueryMessageProcessor {
	var queryMessageProcessor = new(QueryMessageProcessor)
	queryMessageProcessor.BrokerController = brokerController
	return queryMessageProcessor
}

// ProcessRequest 请求
// Author rongzhihong
// Since 2017/9/18
func (qmp *QueryMessageProcessor) ProcessRequest(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {

	switch request.Code {
	case code.QUERY_MESSAGE:
		return qmp.QueryMessage(ctx, request)
	case code.VIEW_MESSAGE_BY_ID:
		return qmp.ViewMessageById(ctx, request)
	}
	return nil, nil
}

// ProcessRequest 查询请求
// Author rongzhihong
// Since 2017/9/18
func (qmp *QueryMessageProcessor) QueryMessage(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &header.QueryMessageResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	requestHeader := &header.QueryMessageRequestHeader{}
	err := response.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	response.Opaque = request.Opaque
	queryMessageResult := qmp.BrokerController.MessageStore.QueryMessage(requestHeader.Topic, requestHeader.Key,
		requestHeader.MaxNum, requestHeader.BeginTimestamp, requestHeader.EndTimestamp)

	if queryMessageResult != nil {
		responseHeader.IndexLastUpdatePhyoffset = queryMessageResult.IndexLastUpdatePhyoffset
		responseHeader.IndexLastUpdateTimestamp = queryMessageResult.IndexLastUpdateTimestamp

		if queryMessageResult.BufferTotalSize > 0 {
			response.Code = code.SUCCESS
			response.Remark = ""

			queryMessageTransfer := pagecache.NewQueryMessageTransfer(response, queryMessageResult)
			_, err := ctx.WriteSerialObject(queryMessageTransfer)
			if err != nil {
				logger.Errorf("transfer query message by pagecache failed, %s", err.Error())
			}
			// TODO queryMessageTransfer.Release()
			return nil, nil
		}
	}

	response.Code = code.QUERY_NOT_FOUND
	response.Remark = "can not find message, maybe time range not correct"
	return response, nil
}

// ProcessRequest 根据MsgId查询消息
// Author rongzhihong
// Since 2017/9/18
func (qmp *QueryMessageProcessor) ViewMessageById(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)
	requestHeader := &header.ViewMessageRequestHeader{}

	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error(err)
	}

	response.Opaque = request.Opaque

	selectMapedBufferResult := qmp.BrokerController.MessageStore.SelectOneMessageByOffset(requestHeader.Offset)
	if selectMapedBufferResult != nil {
		response.Code = code.SUCCESS
		response.Remark = ""

		oneMessageTransfer := pagecache.NewOneMessageTransfer(response, selectMapedBufferResult)
		_, err = ctx.WriteSerialObject(oneMessageTransfer)
		if err != nil {
			logger.Errorf("transfer one message by pagecache failed, %s", err.Error())
		}
		selectMapedBufferResult.Release()
		return nil, nil
	}

	response.Code = code.QUERY_NOT_FOUND
	response.Remark = fmt.Sprintf("can not find message by the offset:%d", requestHeader.Offset)
	return response, nil
}

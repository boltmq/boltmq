// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package server

import (
	"fmt"

	"github.com/boltmq/boltmq/broker/server/pagecache"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/net/core"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/head"
)

// queryMessageProcessor 查询消息请求处理
// Author rongzhihong
// Since 2017/9/18
type queryMessageProcessor struct {
	brokerController *BrokerController
}

// newQueryMessageProcessor 初始化queryMessageProcessor
// Author rongzhihong
// Since 2017/9/18
func newQueryMessageProcessor(brokerController *BrokerController) *queryMessageProcessor {
	var qmp = new(queryMessageProcessor)
	qmp.brokerController = brokerController
	return qmp
}

// ProcessRequest 请求
// Author rongzhihong
// Since 2017/9/18
func (qmp *queryMessageProcessor) ProcessRequest(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {

	switch request.Code {
	case protocol.QUERY_MESSAGE:
		return qmp.QueryMessage(ctx, request)
	case protocol.VIEW_MESSAGE_BY_ID:
		return qmp.ViewMessageById(ctx, request)
	}
	return nil, nil
}

// ProcessRequest 查询请求
// Author rongzhihong
// Since 2017/9/18
func (qmp *queryMessageProcessor) QueryMessage(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	responseHeader := &head.QueryMessageResponseHeader{}
	response := protocol.CreateDefaultResponseCommand(responseHeader)

	requestHeader := &head.QueryMessageRequestHeader{}
	err := response.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("query message err: %s.", err)
	}

	response.Opaque = request.Opaque
	queryMessageResult := qmp.brokerController.messageStore.QueryMessage(requestHeader.Topic, requestHeader.Key,
		requestHeader.MaxNum, requestHeader.BeginTimestamp, requestHeader.EndTimestamp)

	if queryMessageResult != nil {
		responseHeader.IndexLastUpdatePhyoffset = queryMessageResult.IndexLastUpdatePhyoffset
		responseHeader.IndexLastUpdateTimestamp = queryMessageResult.IndexLastUpdateTimestamp

		if queryMessageResult.BufferTotalSize > 0 {
			response.Code = protocol.SUCCESS
			response.Remark = ""

			queryMessageTransfer := pagecache.NewQueryMessageTransfer(response, queryMessageResult)
			_, err := ctx.WriteSerialData(queryMessageTransfer)
			if err != nil {
				logger.Errorf("transfer query message by pagecache failed, %s", err.Error())
			}
			// TODO queryMessageTransfer.Release()
			return nil, nil
		}
	}

	response.Code = protocol.QUERY_NOT_FOUND
	response.Remark = "can not find message, maybe time range not correct"
	return response, nil
}

// ProcessRequest 根据MsgId查询消息
// Author rongzhihong
// Since 2017/9/18
func (qmp *queryMessageProcessor) ViewMessageById(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &head.ViewMessageRequestHeader{}

	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("view message by id err: %s.", err)
	}

	response.Opaque = request.Opaque

	selectMapedBufferResult := qmp.brokerController.messageStore.SelectOneMessageByOffset(int64(requestHeader.Offset))
	if selectMapedBufferResult != nil {
		response.Code = protocol.SUCCESS
		response.Remark = ""

		oneMessageTransfer := pagecache.NewOneMessageTransfer(response, selectMapedBufferResult)
		_, err = ctx.WriteSerialData(oneMessageTransfer)
		if err != nil {
			logger.Errorf("transfer one message by pagecache failed, %s.", err)
		}
		selectMapedBufferResult.Release()
		return nil, nil
	}

	response.Code = protocol.QUERY_NOT_FOUND
	response.Remark = fmt.Sprintf("can not find message by the offset:%d", requestHeader.Offset)
	return response, nil
}

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
	"strings"

	"github.com/boltmq/boltmq/store"
	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/net/core"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/head"
	"github.com/boltmq/common/sysflag"
)

// endTransactionProcessor Commit或Rollback事务
// Author rongzhihong
// Since 2017/9/18
type endTransactionProcessor struct {
	brokerController *BrokerController
}

// newEndTransactionProcessor 初始化endTransactionProcessor
// Author rongzhihong
// Since 2017/9/18
func newEndTransactionProcessor(brokerController *BrokerController) *endTransactionProcessor {
	var etp = new(endTransactionProcessor)
	etp.brokerController = brokerController
	return etp
}

// ProcessRequest 请求
// Author rongzhihong
// Since 2017/9/18
func (etp *endTransactionProcessor) ProcessRequest(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &head.EndTransactionRequestHeader{}

	// 回查应答
	if requestHeader.FromTransactionCheck {
		switch requestHeader.CommitOrRollback {
		// 不提交也不回滚
		case sysflag.TransactionNotType:
			logger.Warnf("check producer[%s] transaction state, but it's pending status.RequestHeader: %#v Remark: %s.",
				parseChannelRemoteAddr(ctx),
				requestHeader,
				request.Remark)
			return nil, nil

			// 提交
		case sysflag.TransactionCommitType:
			logger.Warnf("check producer[%s] transaction state, the producer commit the message. RequestHeader: %#v Remark: %s.",
				parseChannelRemoteAddr(ctx),
				requestHeader,
				request.Remark)

			// 回滚
		case sysflag.TransactionRollbackType:
			logger.Warnf("check producer[%s] transaction state, the producer rollback the message. RequestHeader: %#v Remark: %s.",
				parseChannelRemoteAddr(ctx),
				requestHeader,
				request.Remark)
		default:
			return nil, nil
		}

	} else { // 正常提交回滚

		switch requestHeader.CommitOrRollback {
		// 不提交也不回滚
		case sysflag.TransactionNotType:
			logger.Warnf("check producer[%s] end transaction in sending message,  and it's pending status.RequestHeader: %#v Remark: %s.",
				parseChannelRemoteAddr(ctx),
				requestHeader,
				request.Remark)
			return nil, nil

		case sysflag.TransactionCommitType:
			// to do nothing
		case sysflag.TransactionRollbackType:
			logger.Warnf("check producer[%s] end transaction in sending message, rollback the message.RequestHeader: %#v Remark: %s.",
				parseChannelRemoteAddr(ctx),
				requestHeader,
				request.Remark)

		default:
			return nil, nil
		}
	}

	msgExt := etp.brokerController.messageStore.LookMessageByOffset(requestHeader.CommitLogOffset)
	if msgExt != nil {
		// 校验Producer Group
		pgroupRead, ok := msgExt.Properties[message.PROPERTY_PRODUCER_GROUP]
		if !ok || !strings.EqualFold(pgroupRead, requestHeader.ProducerGroup) {
			response.Code = protocol.SYSTEM_ERROR
			response.Remark = "the producer group wrong"
			return response, nil
		}

		// 校验Transaction State Table Offset
		if msgExt.QueueOffset != requestHeader.TranStateTableOffset {
			response.Code = protocol.SYSTEM_ERROR
			response.Remark = "the transaction state table offset wrong"
			return response, nil
		}

		// 校验Commit Log Offset
		if msgExt.CommitLogOffset != requestHeader.CommitLogOffset {
			response.Code = protocol.SYSTEM_ERROR
			response.Remark = "the commit log offset wrong"
			return response, nil
		}

		msgInner := etp.endMessageTransaction(msgExt)

		var sysFlag int = int(msgInner.SysFlag)
		var ty int = int(requestHeader.CommitOrRollback)
		msgInner.SysFlag = int32(sysflag.ResetTransactionValue(sysFlag, ty))

		msgInner.QueueOffset = requestHeader.TranStateTableOffset
		msgInner.PreparedTransactionOffset = requestHeader.CommitLogOffset
		msgInner.StoreTimestamp = msgExt.StoreTimestamp
		if sysflag.TransactionRollbackType == requestHeader.CommitOrRollback {
			msgInner.Body = nil
		}

		putMessageResult := etp.brokerController.messageStore.PutMessage(msgInner)
		if putMessageResult != nil {
			switch putMessageResult.Status {
			// Success
			case store.PUTMESSAGE_PUT_OK:
			case store.FLUSH_DISK_TIMEOUT:
			case store.FLUSH_SLAVE_TIMEOUT:
			case store.SLAVE_NOT_AVAILABLE:
				response.Code = protocol.SUCCESS
				response.Remark = ""
			case store.CREATE_MAPPED_FILE_FAILED:
				response.Code = protocol.SYSTEM_ERROR
				response.Remark = "create maped file failed."
			case store.MESSAGE_ILLEGAL:
				response.Code = protocol.SYSTEM_ERROR
				response.Remark = "the message is illegal, maybe length not matched."
			case store.SERVICE_NOT_AVAILABLE:
				response.Code = protocol.SYSTEM_ERROR
				response.Remark = "service not available now."
			case store.PUTMESSAGE_UNKNOWN_ERROR:
				response.Code = protocol.SYSTEM_ERROR
				response.Remark = "UNKNOWN_ERROR"
			default:
				response.Code = protocol.SYSTEM_ERROR
				response.Remark = "UNKNOWN_ERROR DEFAULT"
			}
			return response, nil

		} else {
			response.Code = protocol.SYSTEM_ERROR
			response.Remark = "store putMessage return null"
		}
	} else {
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = "find prepared transaction message failed"
		return response, nil
	}

	return response, nil
}

// endMessageTransactions 消息事务
// Author rongzhihong
// Since 2017/9/18
func (etp *endTransactionProcessor) endMessageTransaction(msgExt *message.MessageExt) *store.MessageExtInner {
	msgInner := &store.MessageExtInner{}
	msgInner.Body = msgExt.Body
	msgInner.Flag = msgExt.Flag
	msgInner.Properties = msgExt.Properties

	tagsFlag := msgInner.SysFlag & sysflag.MultiTagsFlag

	var topicFilterType basis.TopicFilterType
	if tagsFlag == sysflag.MultiTagsFlag {
		topicFilterType = basis.MULTI_TAG
	} else {
		topicFilterType = basis.SINGLE_TAG
	}

	tagsCodeValue := basis.TagsString2tagsCode(topicFilterType, msgInner.GetTags())
	msgInner.TagsCode = tagsCodeValue
	msgInner.PropertiesString = message.MessageProperties2String(msgExt.Properties)

	msgInner.SysFlag = msgExt.SysFlag
	msgInner.BornTimestamp = msgExt.BornTimestamp
	msgInner.BornHost = msgExt.BornHost
	msgInner.StoreHost = msgExt.StoreHost
	msgInner.ReconsumeTimes = msgExt.ReconsumeTimes

	msgInner.SetWaitStoreMsgOK(false)
	if msgInner.Properties != nil {
		delete(msgInner.Properties, message.PROPERTY_DELAY_TIME_LEVEL)
	}

	msgInner.Topic = msgExt.Topic
	msgInner.QueueId = msgExt.QueueId

	return msgInner
}

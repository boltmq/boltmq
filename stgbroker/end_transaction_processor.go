package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	commonprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/remotingUtil"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
	"strings"
)

// EndTransactionProcessor Commit或Rollback事务
// Author rongzhihong
// Since 2017/9/18
type EndTransactionProcessor struct {
	BrokerController *BrokerController
}

// NewEndTransactionProcessor 初始化EndTransactionProcessor
// Author rongzhihong
// Since 2017/9/18
func NewEndTransactionProcessor(brokerController *BrokerController) *PullMessageProcessor {
	var pullMessageProcessor = new(PullMessageProcessor)
	pullMessageProcessor.BrokerController = brokerController
	return pullMessageProcessor
}

// ProcessRequest 请求
// Author rongzhihong
// Since 2017/9/18
func (etp *EndTransactionProcessor) ProcessRequest(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(nil)
	requestHeader := &header.EndTransactionRequestHeader{}

	// 回查应答
	if requestHeader.FromTransactionCheck {
		switch requestHeader.CommitOrRollback {
		// 不提交也不回滚
		case sysflag.TransactionNotType:
			logger.Warnf("check producer[%s] transaction state, but it's pending status.RequestHeader: %#v Remark: %s",
				remotingUtil.ParseChannelRemoteAddr(ctx),
				requestHeader,
				request.Remark)
			return nil, nil

			// 提交
		case sysflag.TransactionCommitType:
			logger.Warnf("check producer[%s] transaction state, the producer commit the message. RequestHeader: %#v Remark: %s",
				remotingUtil.ParseChannelRemoteAddr(ctx),
				requestHeader,
				request.Remark)

			// 回滚
		case sysflag.TransactionRollbackType:
			logger.Warnf("check producer[%s] transaction state, the producer rollback the message. RequestHeader: %#v Remark: %s",
				remotingUtil.ParseChannelRemoteAddr(ctx),
				requestHeader,
				request.Remark)
		default:
			return nil, nil
		}

	} else { // 正常提交回滚

		switch requestHeader.CommitOrRollback {
		// 不提交也不回滚
		case sysflag.TransactionNotType:
			logger.Warnf("check producer[%s] end transaction in sending message,  and it's pending status.RequestHeader: %#v Remark: %s",
				remotingUtil.ParseChannelRemoteAddr(ctx),
				requestHeader,
				request.Remark)
			return nil, nil

		case sysflag.TransactionCommitType:
			// to do nothing
		case sysflag.TransactionRollbackType:
			logger.Warnf("check producer[%s] end transaction in sending message, rollback the message.RequestHeader: %#v Remark: %s",
				remotingUtil.ParseChannelRemoteAddr(ctx),
				requestHeader,
				request.Remark)

		default:
			return nil, nil
		}
	}

	msgExt := etp.BrokerController.MessageStore.LookMessageByOffset(requestHeader.CommitLogOffset)
	if msgExt != nil {
		// 校验Producer Group
		pgroupRead, ok := msgExt.Properties[message.PROPERTY_PRODUCER_GROUP]
		if !ok || !strings.EqualFold(pgroupRead, requestHeader.ProducerGroup) {
			response.Code = commonprotocol.SYSTEM_ERROR
			response.Remark = "the producer group wrong"
			return response, nil
		}

		// 校验Transaction State Table Offset
		if msgExt.QueueOffset != requestHeader.TranStateTableOffset {
			response.Code = commonprotocol.SYSTEM_ERROR
			response.Remark = "the transaction state table offset wrong"
			return response, nil
		}

		// 校验Commit Log Offset
		if msgExt.CommitLogOffset != requestHeader.CommitLogOffset {
			response.Code = commonprotocol.SYSTEM_ERROR
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

		putMessageResult := etp.BrokerController.MessageStore.PutMessage(msgInner)
		if putMessageResult != nil {
			switch putMessageResult.PutMessageStatus {
			// Success
			case stgstorelog.PUTMESSAGE_PUT_OK:
			case stgstorelog.FLUSH_DISK_TIMEOUT:
			case stgstorelog.FLUSH_SLAVE_TIMEOUT:
			case stgstorelog.SLAVE_NOT_AVAILABLE:
				response.Code = commonprotocol.SUCCESS
				response.Remark = ""
			case stgstorelog.CREATE_MAPEDFILE_FAILED:
				response.Code = commonprotocol.SYSTEM_ERROR
				response.Remark = "create maped file failed."
			case stgstorelog.MESSAGE_ILLEGAL:
				response.Code = commonprotocol.SYSTEM_ERROR
				response.Remark = "the message is illegal, maybe length not matched."
			case stgstorelog.SERVICE_NOT_AVAILABLE:
				response.Code = commonprotocol.SYSTEM_ERROR
				response.Remark = "service not available now."
			case stgstorelog.PUTMESSAGE_UNKNOWN_ERROR:
				response.Code = commonprotocol.SYSTEM_ERROR
				response.Remark = "UNKNOWN_ERROR"
			default:
				response.Code = commonprotocol.SYSTEM_ERROR
				response.Remark = "UNKNOWN_ERROR DEFAULT"
			}
			return response, nil

		} else {
			response.Code = commonprotocol.SYSTEM_ERROR
			response.Remark = "store putMessage return null"
		}
	} else {
		response.Code = commonprotocol.SYSTEM_ERROR
		response.Remark = "find prepared transaction message failed"
		return response, nil
	}

	return response, nil
}

// endMessageTransactions 消息事务
// Author rongzhihong
// Since 2017/9/18
func (etp *EndTransactionProcessor) endMessageTransaction(msgExt *message.MessageExt) *stgstorelog.MessageExtBrokerInner {
	msgInner := &stgstorelog.MessageExtBrokerInner{}
	msgInner.Body = msgExt.Body
	msgInner.Flag = msgExt.Flag
	msgInner.Properties = msgExt.Properties

	tagsFlag := msgInner.SysFlag & sysflag.MultiTagsFlag

	var topicFilterType stgcommon.TopicFilterType
	if tagsFlag == sysflag.MultiTagsFlag {
		topicFilterType = stgcommon.MULTI_TAG
	} else {
		topicFilterType = stgcommon.SINGLE_TAG
	}

	tagsCodeValue := stgstorelog.TagsString2tagsCode(&topicFilterType, msgInner.GetTags())
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

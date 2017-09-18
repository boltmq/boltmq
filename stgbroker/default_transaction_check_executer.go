package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
)

// DefaultTransactionCheckExecuter 存储层回调此接口，用来主动回查Producer的事务状态
// Author rongzhihong
// Since 2017/9/17
type DefaultTransactionCheckExecuter struct {
	brokerController *BrokerController
}

func NewDefaultTransactionCheckExecuter(brokerController *BrokerController) *DefaultTransactionCheckExecuter {
	trans := new(DefaultTransactionCheckExecuter)
	trans.brokerController = brokerController
	return trans
}

func (trans *DefaultTransactionCheckExecuter) GotoCheck(producerGroupHashCode int, tranStateTableOffset,
	commitLogOffset int64, msgSize int) {
	// 第一步、查询Producer
	clientChannelInfo := trans.brokerController.ProducerManager.PickProducerChannelRandomly(producerGroupHashCode)
	if clientChannelInfo == nil {
		logger.Warnf("check a producer transaction state, but not find any channel of this group[%d]",
			producerGroupHashCode)
		return
	}

	// 第二步、查询消息
	// TODO selectMapedBufferResult := trans.brokerController.MessageStore.selectOneMessageByOffset(commitLogOffset, msgSize)
	selectMapedBufferResult := new(stgstorelog.SelectMapedBufferResult)
	if selectMapedBufferResult == nil {
		logger.Warnf("check a producer transaction state, but not find message by commitLogOffset: %d, msgSize: %d",
			commitLogOffset, msgSize)
		return
	}

	// 第三步、向Producer发起请求
	requestHeader := &header.CheckTransactionStateRequestHeader{}
	requestHeader.CommitLogOffset = commitLogOffset
	requestHeader.TranStateTableOffset = tranStateTableOffset

	trans.brokerController.Broker2Client.CheckProducerTransactionState(clientChannelInfo.Conn, requestHeader, selectMapedBufferResult)
}

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
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/protocol/head"
)

type transactionCheckSupervisor struct {
	brokerController *BrokerController
}

func newTransactionCheckSupervisor(brokerController *BrokerController) *transactionCheckSupervisor {
	trans := new(transactionCheckSupervisor)
	trans.brokerController = brokerController
	return trans
}

// gotoCheck 回调检查方法
// Author rongzhihong
// Since 2017/9/17
func (trans *transactionCheckSupervisor) gotoCheck(producerGroupHashCode int, tranStateTableOffset, commitLogOffset int64, msgSize int) {
	// 第一步、查询Producer
	clientChannelInfo := trans.brokerController.prcManager.pickProducerChannelRandomly(producerGroupHashCode)
	if clientChannelInfo == nil {
		logger.Warnf("check a producer transaction state, but not find any channel of this group[%d].",
			producerGroupHashCode)
		return
	}

	// 第二步、查询消息
	selectMapedBufferResult := trans.brokerController.messageStore.SelectOneMessageByOffsetAndSize(commitLogOffset, int32(msgSize))
	if selectMapedBufferResult == nil {
		logger.Warnf("check a producer transaction state, but not find message by commitLogOffset: %d, msgSize: %d.",
			commitLogOffset, msgSize)
		return
	}

	// 第三步、向Producer发起请求
	requestHeader := &head.CheckTransactionStateRequestHeader{}
	requestHeader.CommitLogOffset = commitLogOffset
	requestHeader.TranStateTableOffset = tranStateTableOffset

	trans.brokerController.b2Client.checkProducerTransactionState(clientChannelInfo.ctx, requestHeader, selectMapedBufferResult)
}

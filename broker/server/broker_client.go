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
	"strings"

	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/boltmq/store"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/net/core"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/body"
	"github.com/boltmq/common/protocol/head"
)

// broker2Client Broker主动调用客户端接口
// Author gaoyanlei
// Since 2017/8/9
type broker2Client struct {
	brokerController *BrokerController
}

// newBroker2Client broker2Client
// Author gaoyanlei
// Since 2017/8/9
func newBroker2Client(brokerController *BrokerController) *broker2Client {
	var b2c = new(broker2Client)
	b2c.brokerController = brokerController
	return b2c
}

// notifyConsumerIdsChanged 消费Id 列表改变通知
// Author rongzhihong
// Since 2017/9/11
func (b2c *broker2Client) notifyConsumerIdsChanged(ctx core.Context, consumerGroup string) {
	if "" == consumerGroup {
		logger.Error("notifyConsumerIdsChanged consumerGroup is nil.")
		return
	}

	requestHeader := &head.NotifyConsumerIdsChangedRequestHeader{ConsumerGroup: consumerGroup}
	request := protocol.CreateRequestCommand(protocol.NOTIFY_CONSUMER_IDS_CHANGED, requestHeader)
	request.MarkOnewayRPC()
	b2c.brokerController.remotingServer.InvokeOneway(ctx, request, 10)
}

// checkProducerTransactionState Broker主动回查Producer事务状态，Oneway
// Author rongzhihong
// Since 2017/9/11
func (b2c *broker2Client) checkProducerTransactionState(channel core.Context, requestHeader *head.CheckTransactionStateRequestHeader,
	bufferResult store.BufferResult) {
	request := protocol.CreateRequestCommand(protocol.CHECK_TRANSACTION_STATE, requestHeader)
	request.Body = bufferResult.Buffer().Bytes()
	request.MarkOnewayRPC()
	b2c.brokerController.remotingServer.InvokeOneway(channel, request, 1000)
	bufferResult.Release()
}

// callClient 调用客户端
// Author rongzhihong
// Since 2017/9/18
func (b2c *broker2Client) callClient(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	return b2c.brokerController.remotingServer.InvokeSync(ctx, request, 10000)
}

// resetOffset Broker 主动通知 Consumer，offset列表发生变化，需要进行重置
// Author rongzhihong
// Since 2017/9/18
func (b2c *broker2Client) resetOffset(topic, group string, timeStamp int64, isForce bool) *protocol.RemotingCommand {
	response := protocol.CreateDefaultResponseCommand()
	topicConfig := b2c.brokerController.tpConfigManager.selectTopicConfig(topic)
	if topicConfig == nil {
		logger.Errorf("[reset-offset] reset offset failed, no topic in this broker. topic=%s.", topic)
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = fmt.Sprintf("[reset-offset] reset offset failed, no topic in this broker. topic=" + topic)
		return response
	}

	offsetTable := make(map[*message.MessageQueue]int64)
	var writeQueueNums int = int(topicConfig.WriteQueueNums)
	for i := 0; i < writeQueueNums; i++ {
		mq := &message.MessageQueue{}
		mq.BrokerName = b2c.brokerController.cfg.Cluster.BrokerName
		mq.Topic = topic
		mq.QueueId = i

		consumerOffset := b2c.brokerController.csmOffsetManager.queryOffset(group, topic, i)
		if -1 == consumerOffset {
			response.Code = protocol.SYSTEM_ERROR
			response.Remark = fmt.Sprintf("THe consumer group <%s> not exist", group)
			return response
		}

		timeStampOffset := b2c.brokerController.messageStore.OffsetInQueueByTime(topic, int32(i), timeStamp)
		if isForce || timeStampOffset < consumerOffset {
			offsetTable[mq] = timeStampOffset
		} else {
			offsetTable[mq] = consumerOffset
		}
	}

	requestHeader := &head.ResetOffsetRequestHeader{}
	requestHeader.Topic = topic
	requestHeader.Group = group
	requestHeader.IsForce = isForce
	requestHeader.Timestamp = timeStamp

	request := protocol.CreateRequestCommand(protocol.RESET_CONSUMER_CLIENT_OFFSET, requestHeader)
	resetOffsetBody := body.NewResetOffset()
	resetOffsetBody.OffsetTable = offsetTable

	content, err := common.Encode(resetOffsetBody)
	if err != nil {
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = err.Error()
		return response
	}
	request.Body = content

	request.MarkOnewayRPC()

	cgi := b2c.brokerController.csmManager.getConsumerGroupInfo(group)
	// Consumer在线
	if cgi != nil && len(cgi.getAllChannel()) > 0 {
		chanInfoTable := cgi.connTable
		iterator := chanInfoTable.Iterator()
		for iterator.HasNext() {
			_, val, _ := iterator.Next()
			if chanInfo, ok := val.(*channelInfo); ok {
				b2c.brokerController.remotingServer.InvokeSync(chanInfo.ctx, request, 5000)

				logger.Infof("[reset-offset] reset offset success. topic=%s, group=%s, clientId=%d",
					topic, group, chanInfo.clientId)
			}
		}

	} else {
		// Consumer不在线
		errorInfo := fmt.Sprintf("Consumer not online, so can not reset offset, Group: %s Topic: %s Timestamp: %d",
			requestHeader.Group, requestHeader.Topic, requestHeader.Timestamp)
		logger.Error("reset offset err: %s.", errorInfo)
		response.Code = protocol.CONSUMER_NOT_ONLINE
		response.Remark = errorInfo
		return response
	}

	response.Code = protocol.SUCCESS
	resBody := body.NewResetOffset()
	resBody.OffsetTable = offsetTable
	body, err := common.Encode(resBody)
	if err != nil {
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = err.Error()
		return response
	}

	response.Body = body
	return response
}

// getConsumeStatus Broker主动获取Consumer端的消息情况
// Author rongzhihong
// Since 2017/9/18
func (b2c *broker2Client) getConsumeStatus(topic, group, originClientId string) *protocol.RemotingCommand {
	response := protocol.CreateDefaultResponseCommand()

	requestHeader := &head.GetConsumerStatusRequestHeader{}
	requestHeader.Topic = topic
	requestHeader.Group = group

	request := protocol.CreateRequestCommand(protocol.GET_CONSUMER_STATUS_FROM_CLIENT, requestHeader)

	consumerStatusTable := make(map[string]map[*message.MessageQueue]int64)

	cgi := b2c.brokerController.csmManager.getConsumerGroupInfo(group)
	if nil == cgi {
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = fmt.Sprintf("No Any Consumer online in the consumer group: [%s]", group)
		return response
	}

	chanInfoTable := cgi.connTable
	if nil == chanInfoTable || chanInfoTable.Size() <= 0 {
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = fmt.Sprintf("No Any Consumer online in the consumer group: [%s]", group)
		return response
	}

	for iterator := chanInfoTable.Iterator(); iterator.HasNext(); {
		_, value, _ := iterator.Next()
		chanInfo, vok := value.(*channelInfo)
		if !vok {
			logger.Warnf("The value=%v type is not ChannelInfo.", value)
			continue
		}

		clientId := chanInfo.clientId
		if common.IsBlank(originClientId) || strings.EqualFold(originClientId, clientId) {
			// 不指定 originClientId 则对所有的 client 进行处理；若指定 originClientId 则只对当前
			// originClientId 进行处理
			response, err := b2c.brokerController.remotingServer.InvokeSync(chanInfo.ctx, request, 5000)
			if err != nil {
				logger.Errorf("getConsumeStatus InvokeSync RemoteAddr: %s, err: %s.", chanInfo.ctx.UniqueSocketAddr(), err)
			}
			switch response.Code {
			case protocol.SUCCESS:
				if response.Body != nil && len(response.Body) > 0 {
					statusBody := body.NewGetConsumerStatus()
					common.Decode(response.Body, statusBody)

					consumerStatusTable[clientId] = statusBody.MessageQueueTable
					logger.Infof("[get-consumer-status] get consumer status success. topic=%s, group=%s, channelRemoteAddr=%s.",
						topic, group, clientId)
				}
			}

			// 若指定 originClientId 相应的 client 处理完成，则退出循环
			if !common.IsBlank(originClientId) && strings.EqualFold(originClientId, clientId) {
				break
			}
		}
	}

	resBody := body.NewGetConsumerStatus()
	resBody.ConsumerTable = consumerStatusTable
	content, err := common.Encode(resBody)
	if err != nil {
		response.Code = protocol.SYSTEM_ERROR
		response.Remark = err.Error()
		return response
	}

	response.Code = protocol.SUCCESS
	response.Body = content
	response.Remark = ""
	return response
}

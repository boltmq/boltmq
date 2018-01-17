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
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/boltmq/boltmq/broker/server/longpolling"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/system"
	concurrent "github.com/fanliao/go-concurrentMap"
)

// pullRequestHoldService 拉消息请求管理，如果拉不到消息，则在这里Hold住，等待消息到来
// Author rongzhihong
// Since 2017/9/5
type pullRequestHoldService struct {
	topicQueueIdSeparator string
	pullRequestTable      *concurrent.ConcurrentMap // key:topic@queueid value:ManyPullRequest
	brokerController      *BrokerController
	isStopped             bool
}

// newPullRequestHoldService 初始化拉消息请求服务
// Author rongzhihong
// Since 2017/9/5
func newPullRequestHoldService(brokerController *BrokerController) *pullRequestHoldService {
	serv := new(pullRequestHoldService)
	serv.pullRequestTable = concurrent.NewConcurrentMap()
	serv.topicQueueIdSeparator = TOPIC_GROUP_SEPARATOR
	serv.brokerController = brokerController
	return serv
}

// buildKey 构造Key
// Author rongzhihong
// Since 2017/9/5
func (serv *pullRequestHoldService) buildKey(topic string, queueId int32) string {
	sb := &bytes.Buffer{}
	sb.WriteString(topic)
	sb.WriteString(serv.topicQueueIdSeparator)
	sb.WriteString(fmt.Sprintf("%d", queueId))
	return sb.String()
}

// suspendPullRequest 延缓拉请求
// Author rongzhihong
// Since 2017/9/5
func (serv *pullRequestHoldService) suspendPullRequest(topic string, queueId int32, pullRequest *longpolling.PullRequest) {
	key := serv.buildKey(topic, queueId)
	mpr, err := serv.pullRequestTable.Get(key)
	if err != nil {
		logger.Error("suspend pull request err: %s.", err)
		return
	}

	if nil == mpr {
		mpr = new(longpolling.ManyPullRequest)
		prev, _ := serv.pullRequestTable.PutIfAbsent(key, mpr)
		if prev != nil {
			mpr = prev
		}
	}

	if bean, ok := mpr.(*longpolling.ManyPullRequest); ok {
		bean.AddPullRequest(pullRequest)
	}
}

// checkHoldRequest  检查拉请求是否有数据，如有，则通知
// Author rongzhihong
// Since 2017/9/5
func (serv *pullRequestHoldService) checkHoldRequest() {
	for iter := serv.pullRequestTable.Iterator(); iter.HasNext(); {
		key, _, _ := iter.Next()
		if item, ok := key.(string); ok {

			kArray := strings.Split(item, serv.topicQueueIdSeparator)
			if kArray != nil && 2 == len(kArray) {
				topic := kArray[0]
				queueId, err := strconv.Atoi(kArray[1])
				if err != nil {
					logger.Errorf("queueId=%s: string to int fail.", kArray[1])
					continue
				}
				offset := serv.brokerController.messageStore.MaxOffsetInQueue(topic, int32(queueId))
				serv.notifyMessageArriving(topic, int32(queueId), offset)
			}
		}
	}
}

// notifyMessageArriving  消息到来通知
// Author rongzhihong
// Since 2017/9/5
func (serv *pullRequestHoldService) notifyMessageArriving(topic string, queueId int32, maxOffset int64) {
	key := serv.buildKey(topic, queueId)
	mpr, err := serv.pullRequestTable.Get(key)
	if err != nil {
		logger.Error("notify message arriving err: %s.", err)
		return
	}
	if mpr == nil {
		return
	}

	if mpr, ok := mpr.(*longpolling.ManyPullRequest); ok {
		requestList := mpr.CloneListAndClear()
		if requestList == nil || len(requestList) <= 0 {
			return
		}

		replayList := []*longpolling.PullRequest{}
		for _, pullRequest := range requestList {
			// 查看是否offset OK
			if maxOffset > pullRequest.PullFromThisOffset {
				serv.brokerController.pullMsgProcessor.executeRequestWhenWakeup(pullRequest.Context, pullRequest.RequestCommand)
				continue
			} else {
				// 尝试取最新Offset
				newestOffset := serv.brokerController.messageStore.MaxOffsetInQueue(topic, queueId)
				if newestOffset > pullRequest.PullFromThisOffset {
					serv.brokerController.pullMsgProcessor.executeRequestWhenWakeup(pullRequest.Context, pullRequest.RequestCommand)
					continue
				}
			}

			currentTimeMillis := system.CurrentTimeMillis()
			// 查看是否超时
			if currentTimeMillis >= (pullRequest.SuspendTimestamp + pullRequest.TimeoutMillis) {
				serv.brokerController.pullMsgProcessor.executeRequestWhenWakeup(pullRequest.Context, pullRequest.RequestCommand)
				continue
			}

			// 当前不满足要求，重新放回Hold列表中
			replayList = append(replayList, pullRequest)
		}

		if len(replayList) > 0 {
			mpr.AddManyPullRequest(replayList)
		}
	}
}

// run  运行入口
// Author rongzhihong
// Since 2017/9/5
func (serv *pullRequestHoldService) run() {
	logger.Info("pull request hold service started.")

	for !serv.isStopped {
		time.Sleep(time.Millisecond * time.Duration(1000))
		serv.checkHoldRequest()
	}

	logger.Info("pull request hold service end.")
}

// start  启动入口
// Author rongzhihong
// Since 2017/9/5
func (serv *pullRequestHoldService) start() {
	go func() {
		serv.run()
	}()
	logger.Info("pull request hold service start success.")
}

// shutdown  停止
// Author rongzhihong
// Since 2017/9/5
func (serv *pullRequestHoldService) shutdown() {
	serv.isStopped = true
	logger.Info("pull request hold service shutdown success.")
}

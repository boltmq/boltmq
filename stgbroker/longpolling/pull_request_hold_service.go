package longpolling

import (
	"bytes"
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"strconv"
	"strings"
	"time"
)

const TOPIC_GROUP_SEPARATOR = "@"

// PullRequestHoldService 拉消息请求管理，如果拉不到消息，则在这里Hold住，等待消息到来
// Author rongzhihong
// Since 2017/9/5
type PullRequestHoldService struct {
	TOPIC_QUEUEID_SEPARATOR string
	pullRequestTable        *PullRequestTable
	brokerController        *stgbroker.BrokerController
	isStopped               bool
}

// NewPullRequestHoldService 初始化拉消息请求服务
// Author rongzhihong
// Since 2017/9/5
func (serv *PullRequestHoldService) NewPullRequestHoldService(brokerController *stgbroker.BrokerController) *PullRequestHoldService {
	holdServ := new(PullRequestHoldService)
	holdServ.pullRequestTable = NewPullRequestTable()
	holdServ.TOPIC_QUEUEID_SEPARATOR = TOPIC_GROUP_SEPARATOR
	holdServ.brokerController = brokerController
	return holdServ
}

// buildKey 构造Key
// Author rongzhihong
// Since 2017/9/5
func (serv *PullRequestHoldService) buildKey(topic string, queueId int64) string {
	sb := bytes.Buffer{}
	sb.WriteString(topic)
	sb.WriteString(serv.TOPIC_QUEUEID_SEPARATOR)
	sb.WriteString(strconv.FormatInt(queueId, 10))
	return sb.String()
}

// SuspendPullRequest 延缓拉请求
// Author rongzhihong
// Since 2017/9/5
func (serv *PullRequestHoldService) SuspendPullRequest(topic string, queueId int64, pullRequest *PullRequest) {
	key := serv.buildKey(topic, queueId)
	mpr := serv.pullRequestTable.get(key)
	if nil == mpr {
		mpr = new(ManyPullRequest)
		prev := serv.pullRequestTable.putIfAbsent(key, mpr)
		if prev != nil {
			mpr = prev
		}
	}
	mpr.AddPullRequest(pullRequest)
}

// checkHoldRequest  检查拉请求是否有数据，如有，则通知
// Author rongzhihong
// Since 2017/9/5
func (serv *PullRequestHoldService) checkHoldRequest() {
	for k, _ := range serv.pullRequestTable.PullRequestMap {
		kArray := strings.Split(k, serv.TOPIC_QUEUEID_SEPARATOR)
		if 2 == len(kArray) {
			topic := kArray[0]
			queueId, _ := strconv.ParseInt(kArray[1], 10, 64)
			// TODO long offset =  this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, queueId);
			offset := int64(0)
			serv.notifyMessageArriving(topic, queueId, offset)
		}
	}
}

// notifyMessageArriving  消息到来通知
// Author rongzhihong
// Since 2017/9/5
func (serv *PullRequestHoldService) notifyMessageArriving(topic string, queueId, maxOffset int64) {
	key := serv.buildKey(topic, queueId)
	mpr := serv.pullRequestTable.get(key)
	if mpr != nil {
		requestList := mpr.CloneListAndClear()
		if requestList != nil {
			replayList := []*PullRequest{}

			for _, pullRequest := range requestList {
				// 查看是否offset OK
				if maxOffset > pullRequest.pullFromThisOffset {
					// TODO brokerController.getPullMessageProcessor().excuteRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
					continue
				} else {
					// 尝试取最新Offset
					// newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, queueId)
					newestOffset := int64(0)
					if newestOffset > pullRequest.pullFromThisOffset {
						// TODO brokerController.getPullMessageProcessor().excuteRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
						continue
					}
				}

				// 查看是否超时
				if time.Now().Unix() >= (pullRequest.suspendTimestamp + pullRequest.timeoutMillis) {
					// TODO: brokerController.getPullMessageProcessor().excuteRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
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
}

// run  运行入口
// Author rongzhihong
// Since 2017/9/5
func (serv *PullRequestHoldService) run() {
	defer utils.RecoveredFn()
	logger.Info(serv.getServiceName() + " service started")

	for !serv.isStopped {
		time.Sleep(time.Millisecond * time.Duration(1000))
		serv.checkHoldRequest()
	}

	logger.Info(serv.getServiceName() + " service end")
}

// Start  启动入口
// Author rongzhihong
// Since 2017/9/5
func (serv *PullRequestHoldService) Start() {
	go func() {
		serv.run()
	}()
}

// Shutdown  停止
// Author rongzhihong
// Since 2017/9/5
func (serv *PullRequestHoldService) Shutdown() {
	serv.isStopped = true
}

// getServiceName  获得类名
// Author rongzhihong
// Since 2017/9/5
func (serv *PullRequestHoldService) getServiceName() string {
	return "PullRequestHoldService"
}

package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"strings"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
)
// PullAPIWrapper: pull包装类
// Author: yintongqiang
// Since:  2017/8/11

type PullAPIWrapper struct {
	pullFromWhichNodeTable *sync.Map
	mQClientFactory        *MQClientInstance
	consumerGroup          string
	unitMode               bool
	connectBrokerByUser    bool
	defaultBrokerId        int
}

func NewPullAPIWrapper(mQClientFactory *MQClientInstance, consumerGroup string, unitMode bool) *PullAPIWrapper {
	return &PullAPIWrapper{
		pullFromWhichNodeTable:sync.NewMap(),
		mQClientFactory:mQClientFactory,
		consumerGroup:consumerGroup,
		unitMode:unitMode,
		defaultBrokerId:stgcommon.MASTER_ID }
}

func (api *PullAPIWrapper) PullKernelImpl(mq *message.MessageQueue,
subExpression string,
subVersion int,
offset int64,
maxNums int,
sysFlag int,
commitOffset int64,
brokerSuspendMaxTimeMillis int,
timeoutMillis int,
communicationMode CommunicationMode,
pullCallback consumer.PullCallback) consumer.PullResult {
	findBrokerResult := api.mQClientFactory.findBrokerAddressInSubscribe(mq.BrokerName, api.recalculatePullFromWhichNode(mq), false)
	if strings.EqualFold(findBrokerResult.brokerAddr, "") {
		api.mQClientFactory.UpdateTopicRouteInfoFromNameServerByTopic(mq.Topic)
		findBrokerResult = api.mQClientFactory.findBrokerAddressInSubscribe(mq.BrokerName, api.recalculatePullFromWhichNode(mq), false)
	}
	if !strings.EqualFold(findBrokerResult.brokerAddr, "") {
		sysFlagInner := sysFlag
		if findBrokerResult.slave {
			sysFlagInner = sysflag.ClearCommitOffsetFlag(sysFlagInner)
		}
		requestHeader := header.PullMessageRequestHeader{
			ConsumerGroup:api.consumerGroup,
			Topic:mq.Topic,
			QueueId:mq.QueueId,
			QueueOffset:offset,
			MaxMsgNums:maxNums,
			SysFlag:sysFlagInner,
			CommitOffset:commitOffset,
			SuspendTimeoutMillis:brokerSuspendMaxTimeMillis,
			Subscription:subExpression,
			SubVersion:subVersion}
		brokerAddr := findBrokerResult.brokerAddr
		//todo filter处理
		pullResult := api.mQClientFactory.MQClientAPIImpl.PullMessage(brokerAddr, requestHeader, timeoutMillis, communicationMode, pullCallback)
		return pullResult

	} else {
		panic("The broker[" + mq.BrokerName + "] not exist")
	}
	return consumer.PullResult{}
}

func (api *PullAPIWrapper) recalculatePullFromWhichNode(mq *message.MessageQueue) int {
	//todo FiltersrvController
	suggest, _ := api.pullFromWhichNodeTable.Get(mq)
	if suggest != nil {
		return suggest.(int)
	}
	return stgcommon.MASTER_ID
}

func (api *PullAPIWrapper) updatePullFromWhichNode(mq *message.MessageQueue,brokerId int) {
	suggest, _ := api.pullFromWhichNodeTable.Get(mq)
	if suggest == nil {
		api.pullFromWhichNodeTable.Put(mq,brokerId)

	}else{
		//todo 考虑value是否应该为指针类型
		api.pullFromWhichNodeTable.Put(mq,brokerId)
	}
}

func (api *PullAPIWrapper) processPullResult(mq *message.MessageQueue,pullResult *consumer.PullResult,subscriptionData heartbeat.SubscriptionData) *consumer.PullResult {
	projectGroupPrefix:=api.mQClientFactory.MQClientAPIImpl.ProjectGroupPrefix
	var pullResultExt =PullResultExt{PullResult:pullResult}
     api.updatePullFromWhichNode(mq,int(pullResultExt.suggestWhichBrokerId))
	// todo 有消息编解码操作
	if consumer.FOUND==pullResult.PullStatus{

	}
	logger.Info(projectGroupPrefix)
	return pullResult
}
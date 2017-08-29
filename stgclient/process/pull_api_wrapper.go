package process

import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"strconv"
	"strings"
)

// PullAPIWrapper: pull包装类
// Author: yintongqiang
// Since:  2017/8/11

type PullAPIWrapper struct {
	pullFromWhichNodeTable *sync.Map // *MessageQueue, int/* brokerId */
	mQClientFactory        *MQClientInstance
	consumerGroup          string
	unitMode               bool
	connectBrokerByUser    bool
	defaultBrokerId        int
}

func NewPullAPIWrapper(mQClientFactory *MQClientInstance, consumerGroup string, unitMode bool) *PullAPIWrapper {
	return &PullAPIWrapper{
		pullFromWhichNodeTable: sync.NewMap(),
		mQClientFactory:        mQClientFactory,
		consumerGroup:          consumerGroup,
		unitMode:               unitMode,
		defaultBrokerId:        stgcommon.MASTER_ID}
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
	pullCallback PullCallback) *PullResultExt {
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
			ConsumerGroup:        api.consumerGroup,
			Topic:                mq.Topic,
			QueueId:              int32(mq.QueueId),
			QueueOffset:          offset,
			MaxMsgNums:           maxNums,
			SysFlag:              sysFlagInner,
			CommitOffset:         commitOffset,
			SuspendTimeoutMillis: brokerSuspendMaxTimeMillis,
			Subscription:         subExpression,
			SubVersion:           subVersion}
		brokerAddr := findBrokerResult.brokerAddr
		//todo filter处理
		pullResultExt := api.mQClientFactory.MQClientAPIImpl.PullMessage(brokerAddr, requestHeader, timeoutMillis, communicationMode, pullCallback)
		return pullResultExt

	} else {
		panic("The broker[" + mq.BrokerName + "] not exist")
	}
	return nil
}

func (api *PullAPIWrapper) recalculatePullFromWhichNode(mq *message.MessageQueue) int {
	//todo FiltersrvController
	suggest, _ := api.pullFromWhichNodeTable.Get(mq)
	if suggest != nil {
		return suggest.(int)
	}
	return stgcommon.MASTER_ID
}

func (api *PullAPIWrapper) updatePullFromWhichNode(mq *message.MessageQueue, brokerId int) {
	suggest, _ := api.pullFromWhichNodeTable.Get(mq)
	if suggest == nil {
		api.pullFromWhichNodeTable.Put(mq, brokerId)

	} else {
		suggest = brokerId
		api.pullFromWhichNodeTable.Put(mq, suggest)
	}
}

func (api *PullAPIWrapper) processPullResult(mq *message.MessageQueue, pullResultExt *PullResultExt, subscriptionData *heartbeat.SubscriptionData) *PullResultExt {
	projectGroupPrefix := api.mQClientFactory.MQClientAPIImpl.ProjectGroupPrefix
	pullResult := pullResultExt.PullResult
	api.updatePullFromWhichNode(mq, int(pullResultExt.suggestWhichBrokerId))
	if consumer.FOUND == pullResult.PullStatus {
		// todo 有消息编解码操作
		var msgListFilterAgain []*message.MessageExt
		// todo 类过滤默认都不执行
		if len(subscriptionData.TagsSet.ToSlice()) != 0 && !subscriptionData.ClassFilterMode {
		}
		if !strings.EqualFold(projectGroupPrefix, "") {
			subscriptionData.Topic = stgclient.ClearProjectGroup(subscriptionData.Topic, projectGroupPrefix)
			mq.Topic = stgclient.ClearProjectGroup(mq.Topic, projectGroupPrefix)
			for _, msgExt := range msgListFilterAgain {
				msgExt.Topic = stgclient.ClearProjectGroup(msgExt.Topic, projectGroupPrefix)
				message.PutProperty(&msgExt.Message, message.PROPERTY_MIN_OFFSET, strconv.FormatInt(pullResult.MinOffset, 10))
				message.PutProperty(&msgExt.Message, message.PROPERTY_MAX_OFFSET, strconv.FormatInt(pullResult.MaxOffset, 10))
			}
		} else {
			for _, msgExt := range msgListFilterAgain {
				message.PutProperty(&msgExt.Message, message.PROPERTY_MIN_OFFSET, strconv.FormatInt(pullResult.MinOffset, 10))
				message.PutProperty(&msgExt.Message, message.PROPERTY_MAX_OFFSET, strconv.FormatInt(pullResult.MaxOffset, 10))
			}
		}
		pullResultExt.MsgFoundList = msgListFilterAgain
	}
	return pullResultExt
}

package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	set "github.com/deckarep/golang-set"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"sync/atomic"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"strings"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
)

// RemoteBrokerOffsetStore: 保存offset到broker
// Author: yintongqiang
// Since:  2017/8/11

type RemoteBrokerOffsetStore struct {
	mQClientFactory *MQClientInstance
	groupName       string
	storeTimesTotal int64
	// MessageQueue int64
	offsetTable     *sync.Map
}

func NewRemoteBrokerOffsetStore(mQClientFactory *MQClientInstance, groupName string) *RemoteBrokerOffsetStore {
	return &RemoteBrokerOffsetStore{mQClientFactory:mQClientFactory, groupName:groupName, offsetTable:sync.NewMap()}
}

func (store *RemoteBrokerOffsetStore)Load() {

}

func (store *RemoteBrokerOffsetStore)PersistAll(mqs set.Set) {
	if len(mqs.ToSlice()) == 0 {
		return
	}
	unusedMQ := set.NewSet()
	times := atomic.AddInt64(&store.storeTimesTotal, 1)
	for ite := store.offsetTable.Iterator(); ite.HasNext(); {
		mq, v, _ := ite.Next()
		if mqs.Contains(mq) {
			store.updateConsumeOffsetToBroker(mq.(message.MessageQueue), v.(int64))
			if times % 12 == 0 {
				logger.Info("Group: %v ClientId: %v updateConsumeOffsetToBroker %v", //
					store.groupName, //
					store.mQClientFactory.ClientId, //
					v.(int64))
			}
		} else {
			unusedMQ.Add(mq)
		}
	}
	if len(unusedMQ.ToSlice()) > 0 {
		for mq := range unusedMQ.Iterator().C {
			store.offsetTable.Remove(mq)
			logger.Info("remove unused mq")
		}
	}
}

func (store *RemoteBrokerOffsetStore)Persist(mq message.MessageQueue) {

}

func (store *RemoteBrokerOffsetStore)updateConsumeOffsetToBroker(mq message.MessageQueue, offset int64) {
	findBrokerResult := store.mQClientFactory.findBrokerAddressInAdmin(mq.BrokerName)
	if strings.EqualFold(findBrokerResult.brokerAddr, "") {
		store.mQClientFactory.UpdateTopicRouteInfoFromNameServerByTopic(mq.Topic)
		findBrokerResult = store.mQClientFactory.findBrokerAddressInAdmin(mq.BrokerName)
	}

	if !strings.EqualFold(findBrokerResult.brokerAddr, "") {
		requestHeader:=header.UpdateConsumerOffsetRequestHeader{
			Topic:mq.Topic,
			ConsumerGroup:store.groupName,
			QueueId:mq.QueueId,
			CommitOffset:offset,
		}
		store.mQClientFactory.MQClientAPIImpl.UpdateConsumerOffsetOneway(findBrokerResult.brokerAddr,requestHeader,1000*5)
	}
}

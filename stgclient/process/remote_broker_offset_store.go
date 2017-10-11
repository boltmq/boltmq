package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	set "github.com/deckarep/golang-set"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"sync/atomic"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"strings"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/store"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
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
	defer func() {
		atomic.AddInt64(&store.storeTimesTotal, 1)
	}()
	if len(mqs.ToSlice()) == 0 {
		return
	}
	unusedMQ := set.NewSet()
	times := store.storeTimesTotal
	for ite := store.offsetTable.Iterator(); ite.HasNext(); {
		mq, v, _ := ite.Next()
		containsFlag := false
		for mqs := range mqs.Iterator().C {
			ms := mqs.(*message.MessageQueue)
			if strings.EqualFold(ms.Topic, mq.(*message.MessageQueue).Topic)&&strings.EqualFold(ms.BrokerName, mq.(*message.MessageQueue).BrokerName) && ms.QueueId == mq.(*message.MessageQueue).QueueId {
				containsFlag = true
				break
			}
		}
		//todo set 不支持指针
		if containsFlag {
			//if mqs.Contains(mq) {
			store.updateConsumeOffsetToBroker(mq.(*message.MessageQueue), v.(int64))
			if times % 12 == 0 {
				logger.Infof("Group: %v ClientId: %v Topic: %v QueueId: %v updateConsumeOffsetToBroker %v", //
					store.groupName, //
					store.mQClientFactory.ClientId, //
					mq.(*message.MessageQueue).Topic,
					mq.(*message.MessageQueue).QueueId,
					v.(int64))
			}
		} else {
			unusedMQ.Add(mq)
		}
	}
	if len(unusedMQ.ToSlice()) > 0 {
		for mq := range unusedMQ.Iterator().C {
			store.offsetTable.Remove(mq)
			logger.Infof("remove unused mq")
		}
	}
}

func (store *RemoteBrokerOffsetStore)Persist(mq *message.MessageQueue) {
	offset, _ := store.offsetTable.Get(mq)
	if offset != nil {
		store.updateConsumeOffsetToBroker(mq, offset.(int64))
	}

}

func (store *RemoteBrokerOffsetStore)updateConsumeOffsetToBroker(mq *message.MessageQueue, offset int64) {
	findBrokerResult := store.mQClientFactory.findBrokerAddressInAdmin(mq.BrokerName)
	if strings.EqualFold(findBrokerResult.brokerAddr, "") {
		store.mQClientFactory.UpdateTopicRouteInfoFromNameServerByTopic(mq.Topic)
		findBrokerResult = store.mQClientFactory.findBrokerAddressInAdmin(mq.BrokerName)
	}

	if !strings.EqualFold(findBrokerResult.brokerAddr, "") {
		requestHeader := header.UpdateConsumerOffsetRequestHeader{
			Topic:mq.Topic,
			ConsumerGroup:store.groupName,
			QueueId:mq.QueueId,
			CommitOffset:offset,
		}
		store.mQClientFactory.MQClientAPIImpl.UpdateConsumerOffsetOneway(findBrokerResult.brokerAddr, requestHeader, 1000 * 5)
	}
}

func (store *RemoteBrokerOffsetStore)RemoveOffset(mq *message.MessageQueue) {
	store.offsetTable.Remove(mq)
	logger.Infof("remove unnecessary messageQueue offset. mq, offsetTableSize")
}

func (rStore *RemoteBrokerOffsetStore)ReadOffset(mq *message.MessageQueue, rType store.ReadOffsetType) int64 {
	switch rType {
	case store.MEMORY_FIRST_THEN_STORE:
	case store.READ_FROM_MEMORY:
		offset, _ := rStore.offsetTable.Get(mq)
		if offset != nil {
			return offset.(int64)
		}
	case store.READ_FROM_STORE:
		brokerOffset := rStore.fetchConsumeOffsetFromBroker(mq)
		rStore.UpdateOffset(mq, brokerOffset, false)
		return brokerOffset

	}
	return -1
}

func (store *RemoteBrokerOffsetStore)UpdateOffset(mq *message.MessageQueue, offset int64, increaseOnly bool) {
	offsetOld, _ := store.offsetTable.Get(mq)
	if offsetOld == nil {
		offsetOld, _ = store.offsetTable.PutIfAbsent(mq, offset)
	} else {
		if increaseOnly {
			var offsetV int64 = offsetOld.(int64)
			ok := stgcommon.CompareAndIncreaseOnly(&offsetV, offset)
			if ok {
				store.offsetTable.Put(mq, offset)
			}
		} else {
			offsetOld = offset
			store.offsetTable.Put(mq, offset)
		}

	}
	return
}

func (store *RemoteBrokerOffsetStore)fetchConsumeOffsetFromBroker(mq *message.MessageQueue) int64 {
	 findBrokerResult:=store.mQClientFactory.findBrokerAddressInAdmin(mq.BrokerName)
	if strings.EqualFold(findBrokerResult.brokerAddr,""){
		store.mQClientFactory.UpdateTopicRouteInfoFromNameServerByTopic(mq.Topic)
		findBrokerResult=store.mQClientFactory.findBrokerAddressInAdmin(mq.BrokerName)
	}
	if !strings.EqualFold(findBrokerResult.brokerAddr,""){
       requestHeader:=header.QueryConsumerOffsetRequestHeader{Topic:mq.Topic,ConsumerGroup:store.groupName,QueueId:int32(mq.QueueId)}
	   return store.mQClientFactory.MQClientAPIImpl.queryConsumerOffset(findBrokerResult.brokerAddr,requestHeader,1000*5)
	}
	return -1
}
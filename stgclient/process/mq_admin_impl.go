package process

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"sort"
	"strings"
)

// MQAdminImpl: 运维方法
// Author: yintongqiang
// Since:  2017/8/13
type MQAdminImpl struct {
	mQClientFactory *MQClientInstance
}

func NewMQAdminImpl(mQClientFactory *MQClientInstance) *MQAdminImpl {
	return &MQAdminImpl{mQClientFactory: mQClientFactory}
}

func (admin *MQAdminImpl) MaxOffset(mq *message.MessageQueue) int64 {
	brokerAddr := admin.mQClientFactory.FindBrokerAddressInPublish(mq.BrokerName)
	if strings.EqualFold(brokerAddr, "") {
		admin.mQClientFactory.UpdateTopicRouteInfoFromNameServerByTopic(mq.Topic)
		brokerAddr = admin.mQClientFactory.FindBrokerAddressInPublish(mq.BrokerName)
	}
	if !strings.EqualFold(brokerAddr, "") {
		return admin.mQClientFactory.MQClientAPIImpl.GetMaxOffset(brokerAddr, mq.Topic, mq.QueueId, 1000*3)
	} else {
		panic("The broker[" + mq.BrokerName + "] not exist")
	}
	return -1
}

func (admin *MQAdminImpl) CreateTopic(key, newTopic string, queueNum, topicSysFlag int) {
	topicRouteData := admin.mQClientFactory.MQClientAPIImpl.GetTopicRouteInfoFromNameServer(key, 1000*3)
	if topicRouteData == nil {
		format := "topicRouteData is nil, create topic failed. key=%s, newTopic=%s"
		panic(fmt.Sprintf(format, key, newTopic))
		return
	}
	brokerDataList := topicRouteData.BrokerDatas
	if brokerDataList != nil && len(brokerDataList) > 0 {
		var brokers route.BrokerDatas = brokerDataList
		sort.Sort(brokers)
		for _, brokerData := range brokerDataList {
			addr := brokerData.BrokerAddrs[stgcommon.MASTER_ID]
			if !strings.EqualFold(addr, "") {
				topicConfig := stgcommon.TopicConfig{
					TopicName:      newTopic,
					ReadQueueNums:  int32(queueNum),
					WriteQueueNums: int32(queueNum),
					Perm:           constant.PERM_READ | constant.PERM_WRITE,
					TopicSysFlag:   topicSysFlag,
				}
				admin.mQClientFactory.MQClientAPIImpl.CreateTopic(addr, key, topicConfig, 1000*3)
			}
		}
	}
}

func (admin *MQAdminImpl) FetchSubscribeMessageQueues(topic string) []*message.MessageQueue {
	mqList := []*message.MessageQueue{}
	routeData := admin.mQClientFactory.MQClientAPIImpl.GetTopicRouteInfoFromNameServer(topic, 1000*3)
	if routeData != nil {
		mqSet := admin.mQClientFactory.topicRouteData2TopicSubscribeInfo(topic, routeData)
		if mqSet != nil {
			for mq := range mqSet.Iterator().C {
				mqList = append(mqList, mq.(*message.MessageQueue))
			}
		}
	}
	return mqList
}

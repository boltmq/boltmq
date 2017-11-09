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

func NewMQAdminImpl(clientFactory *MQClientInstance) *MQAdminImpl {
	return &MQAdminImpl{mQClientFactory: clientFactory}
}

func (impl *MQAdminImpl) MaxOffset(mq *message.MessageQueue) int64 {
	brokerAddr := impl.mQClientFactory.FindBrokerAddressInPublish(mq.BrokerName)
	if strings.EqualFold(brokerAddr, "") {
		impl.mQClientFactory.UpdateTopicRouteInfoFromNameServerByTopic(mq.Topic)
		brokerAddr = impl.mQClientFactory.FindBrokerAddressInPublish(mq.BrokerName)
	}
	if !strings.EqualFold(brokerAddr, "") {
		return impl.mQClientFactory.MQClientAPIImpl.GetMaxOffset(brokerAddr, mq.Topic, mq.QueueId, 1000*3)
	} else {
		format := fmt.Sprintf("The broker[%s] not exist", mq.BrokerName)
		panic(format)
	}
	return -1
}

func (impl *MQAdminImpl) CreateTopic(key, newTopic string, queueNum, topicSysFlag int) error {
	topicRouteData := impl.mQClientFactory.MQClientAPIImpl.GetTopicRouteInfoFromNameServer(key, 1000*3)
	if topicRouteData == nil {
		format := "topicRouteData is nil, create topic failed. key=%s, newTopic=%s"
		errMsg := fmt.Errorf(format, key, newTopic)
		panic(errMsg)
		return errMsg
	}
	brokerDataList := topicRouteData.BrokerDatas
	if brokerDataList != nil && len(brokerDataList) > 0 {
		var brokers route.BrokerDatas = brokerDataList
		sort.Sort(brokers)
		for _, brokerData := range brokerDataList {
			brokerAddr := brokerData.BrokerAddrs[stgcommon.MASTER_ID]
			if !strings.EqualFold(brokerAddr, "") {
				topicConfig := &stgcommon.TopicConfig{
					TopicName:      newTopic,
					ReadQueueNums:  int32(queueNum),
					WriteQueueNums: int32(queueNum),
					Perm:           constant.PERM_READ | constant.PERM_WRITE,
					TopicSysFlag:   topicSysFlag,
				}
				err := impl.mQClientFactory.MQClientAPIImpl.CreateTopic(brokerAddr, key, topicConfig, 1000*3)
				if err != nil {
					fmt.Printf("%s\n", err.Error())
				}
				return nil
			}
		}
	}
	return fmt.Errorf("create topic failed, unknown reason")
}

func (impl *MQAdminImpl) FetchSubscribeMessageQueues(topic string) []*message.MessageQueue {
	mqList := []*message.MessageQueue{}
	routeData := impl.mQClientFactory.MQClientAPIImpl.GetTopicRouteInfoFromNameServer(topic, 1000*3)
	if routeData != nil {
		mqSet := impl.mQClientFactory.topicRouteData2TopicSubscribeInfo(topic, routeData)
		if mqSet != nil {
			for mq := range mqSet.Iterator().C {
				mqList = append(mqList, mq.(*message.MessageQueue))
			}
		}
	}
	return mqList
}

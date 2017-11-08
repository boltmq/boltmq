package process

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/admin"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	set "github.com/deckarep/golang-set"
)

// CreateCustomTopic 创建指定Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
func (impl *MQClientAPIImpl) CreateCustomTopic(addr, defaultTopic string, topicConfig stgcommon.TopicConfig, timeoutMillis int) {
	//todo
}

// GetTopicListFromNameServer 从Namesrv查询所有Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
func (impl *MQClientAPIImpl) GetTopicListFromNameServer(timeoutMills int64) (*body.TopicList, error) {
	topicList := body.NewTopicList()
	request := protocol.CreateRequestCommand(code.GET_ALL_TOPIC_LIST_FROM_NAMESERVER)
	response, err := impl.DefalutRemotingClient.InvokeSync("", request, timeoutMills)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetTopicListFromNameServer response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetTopicListFromNameServer failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	content := response.Body
	if content == nil || len(content) == 0 {
		return topicList, nil
	}

	err = topicList.CustomDecode(content, topicList)
	if err != nil {
		return nil, err
	}
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) && topicList.TopicList != nil {
		newTopicSet := set.NewSet()
		for topic := range topicList.TopicList.Iterator().C {
			newTopicSet.Add(stgclient.ClearProjectGroup(topic.(string), impl.ProjectGroupPrefix))
		}
		topicList.TopicList = newTopicSet
	}

	return topicList, nil
}

// GetTopicStatsInfo 查询Topic状态信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetTopicStatsInfo(brokerAddr, topic string, timeoutMillis int64) (*admin.TopicStatsTable, error) {
	topicWithProjectGroup := topic
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) {
		topicWithProjectGroup = stgclient.BuildWithProjectGroup(topic, impl.ProjectGroupPrefix)
	}
	requestHeader := header.NewGetTopicStatsInfoRequestHeader(topicWithProjectGroup)
	request := protocol.CreateRequestCommand(code.GET_TOPIC_STATS_INFO, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetTopicStatsInfo response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetTopicStatsInfo failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	topicStatsTable := new(admin.TopicStatsTable)
	content := response.Body
	if content == nil || len(content) == 0 {
		return topicStatsTable, nil
	}

	err = topicStatsTable.CustomDecode(content, topicStatsTable)
	if err != nil {
		return nil, err
	}
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) && topicStatsTable.OffsetTable != nil {
		newTopicOffsetMap := make(map[*message.MessageQueue]*admin.TopicOffset, 256)
		for key, value := range topicStatsTable.OffsetTable {
			if key != nil {
				key.Topic = stgclient.ClearProjectGroup(key.Topic, impl.ProjectGroupPrefix)
				newTopicOffsetMap[key] = value
			}
		}
		topicStatsTable.OffsetTable = newTopicOffsetMap
	}
	return topicStatsTable, nil
}

// GetConsumeStats 查询消费者状态
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetConsumeStatsByTopic(brokerAddr, consumerGroup, topic string, timeoutMillis int64) (*admin.ConsumeStats, error) {
	consumerGroupWithProjectGroup := consumerGroup
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) {
		consumerGroupWithProjectGroup = stgclient.BuildWithProjectGroup(consumerGroup, impl.ProjectGroupPrefix)
	}
	requestHeader := header.NewGetConsumeStatsRequestHeader(consumerGroupWithProjectGroup, topic)
	request := protocol.CreateRequestCommand(code.GET_CONSUME_STATS, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetConsumeStatsByTopic response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetConsumeStats failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}

	consumeStats := new(admin.ConsumeStats)
	content := response.Body
	if content == nil || len(content) == 0 {
		return consumeStats, nil
	}

	err = consumeStats.CustomDecode(content, consumeStats)
	if err != nil {
		return nil, err
	}
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) && consumeStats.OffsetTable != nil {
		newTopicOffsetMap := make(map[*message.MessageQueue]*admin.OffsetWrapper, 256)
		for key, value := range consumeStats.OffsetTable {
			if key != nil {
				key.Topic = stgclient.ClearProjectGroup(key.Topic, impl.ProjectGroupPrefix)
				newTopicOffsetMap[key] = value
			}
		}
		consumeStats.OffsetTable = newTopicOffsetMap
	}
	return consumeStats, nil
}

// GetConsumeStats 查询消费者状态
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetConsumeStats(brokerAddr, consumerGroup string, timeoutMillis int64) (*admin.ConsumeStats, error) {
	return impl.GetConsumeStatsByTopic(brokerAddr, consumerGroup, "", timeoutMillis)
}

// GetProducerConnectionList 查询在线生产者进程信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetProducerConnectionList(brokerAddr, producerGroup string, timeoutMillis int64) (*body.ProducerConnection, error) {
	producerGroupWithProjectGroup := producerGroup
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) {
		producerGroupWithProjectGroup = stgclient.BuildWithProjectGroup(producerGroup, impl.ProjectGroupPrefix)
	}
	requestHeader := header.NewGetProducerConnectionListRequestHeader(producerGroupWithProjectGroup)
	request := protocol.CreateRequestCommand(code.GET_PRODUCER_CONNECTION_LIST, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetProducerConnectionList response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetProducerConnectionList failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	producerConnection := body.NewProducerConnection()
	err = producerConnection.CustomDecode(response.Body, producerConnection)
	return producerConnection, err
}

// GetConsumerConnectionList 查询在线消费进程列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetConsumerConnectionList(brokerAddr, consumerGroup string, timeoutMillis int64) (*body.ConsumerConnection, error) {
	consumerGroupWithProjectGroup := consumerGroup
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) {
		consumerGroupWithProjectGroup = stgclient.BuildWithProjectGroup(consumerGroup, impl.ProjectGroupPrefix)
	}
	requestHeader := header.NewGetConsumerConnectionListRequestHeader(consumerGroupWithProjectGroup)
	request := protocol.CreateRequestCommand(code.GET_CONSUMER_CONNECTION_LIST, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetConsumerConnectionList response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetConsumerConnectionList failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}

	consumerConnection := new(body.ConsumerConnection)
	content := response.Body
	if content == nil || len(content) == 0 {
		return consumerConnection, nil
	}

	err = consumerConnection.CustomDecode(content, consumerConnection)
	if err != nil {
		return nil, err
	}
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) && consumerConnection != nil && consumerConnection.SubscriptionTable != nil {
		subscriptionDataConcurrentHashMap := consumerConnection.SubscriptionTable
		for subscriptionDataEntry := subscriptionDataConcurrentHashMap.Iterator(); subscriptionDataEntry.HasNext(); {
			key, value, _ := subscriptionDataEntry.Next()
			if key == nil || value == nil {
				continue
			}
			topic := key.(string)
			subscriptionData, ok := value.(*heartbeat.SubscriptionData)
			if ok {
				subscriptionData.Topic = stgclient.ClearProjectGroup(topic, impl.ProjectGroupPrefix)
				subscriptionDataConcurrentHashMap.Put(topic, subscriptionData)
			} else {
				logger.Warnf("GetConsumerConnectionList err: %v", subscriptionData)
			}
		}
	}
	return consumerConnection, nil
}

// GetBrokerRuntimeInfo 查询Broker运行时状态信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetBrokerRuntimeInfo(brokerAddr string, timeoutMillis int64) (*body.KVTable, error) {
	request := protocol.CreateRequestCommand(code.GET_BROKER_RUNTIME_INFO)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetBrokerRuntimeInfo response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetBrokerRuntimeInfo failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	kvTable := new(body.KVTable)
	err = kvTable.CustomDecode(response.Body, kvTable)
	return kvTable, err
}

// GetBrokerClusterInfo 查询Cluster集群信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetBrokerClusterInfo(timeoutMillis int64) (*body.ClusterInfo, error) {
	request := protocol.CreateRequestCommand(code.GET_BROKER_CLUSTER_INFO)
	response, err := impl.DefalutRemotingClient.InvokeSync("", request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetBrokerClusterInfo response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetBrokerClusterInfo failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	clusterPlusInfo := body.NewClusterPlusInfo()
	err = clusterPlusInfo.CustomDecode(response.Body, clusterPlusInfo)
	if err != nil {
		logger.Errorf("clusterPlusInfo.CustomDecode() err: %s, %s", err.Error(), response.Body)
		return nil, err
	}

	clusterInfo := clusterPlusInfo.ToClusterInfo()
	return clusterInfo, nil
}

// WipeWritePermOfBroker 关闭broker写权限
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) WipeWritePermOfBroker(namesrvAddr, brokerName string, timeoutMillis int64) (int, error) {
	requestHeader := &namesrv.WipeWritePermOfBrokerRequestHeader{BrokerName: brokerName}
	request := protocol.CreateRequestCommand(code.WIPE_WRITE_PERM_OF_BROKER, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(namesrvAddr, request, timeoutMillis)
	if err != nil {
		return 0, err
	}
	if response == nil {
		return 0, fmt.Errorf("WipeWritePermOfBroker response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("WipeWritePermOfBroker failed. %s", response.ToString())
		return 0, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	responseHeader := new(namesrv.WipeWritePermOfBrokerResponseHeader)
	err = response.DecodeCommandCustomHeader(responseHeader)
	if err != nil {
		return 0, err
	}
	return responseHeader.WipeTopicCount, nil
}

// DeleteTopicInBroker 删除broker节点上对应的Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) DeleteTopicInBroker(brokerAddr, topic string, timeoutMillis int64) error {
	topicWithProjectGroup := topic
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) {
		topicWithProjectGroup = stgclient.BuildWithProjectGroup(topic, impl.ProjectGroupPrefix)
	}
	requestHeader := &header.DeleteTopicRequestHeader{Topic: topicWithProjectGroup}
	request := protocol.CreateRequestCommand(code.DELETE_TOPIC_IN_BROKER, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return err
	}
	if response == nil {
		return fmt.Errorf("DeleteTopicInBroker response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("DeleteTopicInBroker failed. %s", response.ToString())
		return fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	return nil
}

// DeleteTopicInNameServer 删除Namesrv维护的Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) DeleteTopicInNameServer(namesrvAddr, topic string, timeoutMillis int64) error {
	topicWithProjectGroup := topic
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) {
		topicWithProjectGroup = stgclient.BuildWithProjectGroup(topic, impl.ProjectGroupPrefix)
	}
	requestHeader := &header.DeleteTopicRequestHeader{Topic: topicWithProjectGroup}
	request := protocol.CreateRequestCommand(code.DELETE_TOPIC_IN_NAMESRV, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(namesrvAddr, request, timeoutMillis)
	if err != nil {
		return err
	}
	if response == nil {
		return fmt.Errorf("DeleteTopicInNameServer response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("DeleteTopicInNameServer failed. %s", response.ToString())
		return fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	return nil
}

// DeleteSubscriptionGroup 删除订阅组信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) DeleteSubscriptionGroup(brokerAddr, groupName string, timeoutMillis int64) error {
	groupWithProjectGroup := groupName
	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) {
		groupWithProjectGroup = stgclient.BuildWithProjectGroup(groupName, impl.ProjectGroupPrefix)
	}
	requestHeader := &header.DeleteSubscriptionGroupRequestHeader{GroupName: groupWithProjectGroup}
	request := protocol.CreateRequestCommand(code.DELETE_SUBSCRIPTIONGROUP, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return err
	}
	if response == nil {
		return fmt.Errorf("DeleteSubscriptionGroup response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("DeleteSubscriptionGroup failed. %s", response.ToString())
		return fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	return nil
}

// InvokeBrokerToGetConsumerStatus 反向查找broker中的consume状态
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) InvokeBrokerToGetConsumerStatus(brokerAddr, topic, group, clientAddr string, timeoutMillis int64) (map[string]map[*message.MessageQueue]int64, error) {
	requestHeader := header.NewGetConsumerStatusRequestHeader(topic, group, clientAddr)
	request := protocol.CreateRequestCommand(code.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("InvokeBrokerToGetConsumerStatus response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("InvokeBrokerToGetConsumerStatus failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	consumerStatusBody := body.NewGetConsumerStatusBody()
	err = consumerStatusBody.CustomDecode(response.Body, consumerStatusBody)
	if err != nil {
		return nil, err
	}
	if consumerStatusBody == nil {
		return make(map[string]map[*message.MessageQueue]int64), nil
	}
	return consumerStatusBody.ConsumerTable, nil
}

// QueryTopicConsumeByWho 查询topic被那些组消费
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) QueryTopicConsumeByWho(brokerAddr, topic string, timeoutMillis int64) (*body.GroupList, error) {
	requestHeader := &header.QueryTopicConsumeByWhoRequestHeader{Topic: topic}
	request := protocol.CreateRequestCommand(code.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("QueryTopicConsumeByWho response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("QueryTopicConsumeByWho failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	groupList := body.NewGroupList()
	err = groupList.CustomDecode(response.Body, groupList)
	return groupList, err
}

// GetTopicsByCluster 查询集群信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetTopicsByCluster(clusterName string, timeoutMillis int64) (*body.TopicPlusList, error) {
	requestHeader := &header.GetTopicsByClusterRequestHeader{Cluster: clusterName}
	request := protocol.CreateRequestCommand(code.GET_TOPICS_BY_CLUSTER, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync("", request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetTopicsByCluster response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetTopicsByCluster failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}

	topicList := body.NewTopicPlusList()
	err = topicList.CustomDecode(response.Body, topicList)
	if err != nil {
		return nil, err
	}

	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) && topicList.TopicList != nil {
		newTopicSet := set.NewSet()
		for _, topic := range topicList.TopicList {
			newTopicSet.Add(stgclient.ClearProjectGroup(topic, impl.ProjectGroupPrefix))
		}
		//TODO
		topicList.TopicList = []string{}
	}

	if !stgcommon.IsEmpty(impl.ProjectGroupPrefix) && topicList.TopicQueueTable != nil && len(topicList.TopicQueueTable) > 0 {
		for key, value := range topicList.TopicQueueTable {
			topic := stgclient.ClearProjectGroup(key, impl.ProjectGroupPrefix)
			topicList.TopicQueueTable[topic] = value
		}
	}

	return topicList, nil
}

// CleanExpiredConsumeQueue 触发清理失效的消费队列
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (impl *MQClientAPIImpl) CleanExpiredConsumeQueue(brokerAddr string, timeoutMillis int64) (bool, error) {
	request := protocol.CreateRequestCommand(code.CLEAN_EXPIRED_CONSUMEQUEUE)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return false, err
	}
	if response == nil {
		return false, fmt.Errorf("CleanExpiredConsumeQueue response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("CleanExpiredConsumeQueue failed. %s", response.ToString())
		return false, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	return true, nil
}

// GetConsumerRunningInfo 获得consumer运行时状态信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) GetConsumerRunningInfo(brokerAddr, consumerGroup, clientId string, jstack bool, timeoutMillis int64) (*body.ConsumerRunningInfo, error) {
	requestHeader := header.NewGetConsumerRunningInfoRequestHeader(consumerGroup, clientId, jstack)
	request := protocol.CreateRequestCommand(code.GET_CONSUMER_RUNNING_INFO, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetConsumerRunningInfo response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetConsumerRunningInfo failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	consumerRunningInfo := body.NewConsumerRunningInfo()
	err = consumerRunningInfo.CustomDecode(response.Body, consumerRunningInfo)
	return consumerRunningInfo, err
}

// ViewBrokerStatsData 查询broker节点自身的状态信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/3
func (impl *MQClientAPIImpl) ViewBrokerStatsData(brokerAddr, statsName, statsKey string, timeoutMillis int64) (*body.BrokerStatsData, error) {
	requestHeader := &header.ViewBrokerStatsDataRequestHeader{StatsName: statsName, StatsKey: statsKey}
	request := protocol.CreateRequestCommand(code.VIEW_BROKER_STATS_DATA, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("GetTopicsByCluster response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("GetTopicsByCluster failed. %s", response.ToString())
		return nil, fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	brokerStatsData := body.NewBrokerStatsData()
	err = brokerStatsData.CustomDecode(response.Body, brokerStatsData)
	if err != nil {
		return nil, err
	}
	return brokerStatsData, nil
}

// CloneGroupOffset 克隆消费组的偏移量
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (impl *MQClientAPIImpl) CloneGroupOffset(brokerAddr, srcGroup, destGroup, topic string, isOffline bool, timeoutMillis int64) error {
	requestHeader := header.NewCloneGroupOffsetRequestHeader(srcGroup, destGroup, topic, isOffline)
	request := protocol.CreateRequestCommand(code.CLONE_GROUP_OFFSET, requestHeader)
	response, err := impl.DefalutRemotingClient.InvokeSync(brokerAddr, request, timeoutMillis)
	if err != nil {
		return err
	}
	if response == nil {
		return fmt.Errorf("CloneGroupOffset response is nil")
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("CloneGroupOffset failed. %s", response.ToString())
		return fmt.Errorf("%d, %s", response.Code, response.Remark)
	}
	return nil
}

// ConsumeMessageDirectly
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (impl *MQClientAPIImpl) ConsumeMessageDirectly(brokerAddr, consumerGroup, clientId, msgId string, timeoutMills int64) (*body.ConsumeMessageDirectlyResult, error) {
	//TODO:
	return nil, nil
}

// QueryConsumeTimeSpan
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (impl *MQClientAPIImpl) QueryConsumeTimeSpan(brokerAddr, topic, consumerGroup string, timeoutMills int64) (set.Set, error) {
	//TODO:
	return set.NewSet(), nil
}

// GetNameServerAddressList 获取最新namesrv地址列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (impl *MQClientAPIImpl) GetNameServerAddressList() []string {
	return impl.DefalutRemotingClient.GetNameServerAddressList()
}

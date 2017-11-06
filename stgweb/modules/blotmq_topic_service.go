package modules

import (
	"errors"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"sort"
)

type BoltMQTopicService struct {
	AbstractService
}

// List 查询所有Topic列表(不区分topic类型)
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *BoltMQTopicService) GetAllList() (topics []string, err error) {
	defer utils.RecoveredFn()
	service.InitMQAdmin()
	service.Start()
	defer service.Shutdown()

	topicList, err := service.DefaultMQAdminExtImpl.FetchAllTopicList()
	if err != nil {
		logger.Errorf("DefaultMQAdminExtImpl.FetchAllTopicList() err: %s", err.Error())
		return []string{}, err
	}
	if topicList == nil || topicList.TopicList == nil || topicList.TopicList.Cardinality() == 0 {
		return []string{}, errors.New("DefaultMQAdminExtImpl FetchAllTopicList() is blank")
	}
	topics = make([]string, 0, topicList.TopicList.Cardinality())
	for topic := range topicList.TopicList.Iterator().C {
		topics = append(topics, topic.(string))
	}
	logger.Infof("all topic size = %d", len(topics))

	return topics, nil
}

// GetTopicList 根据Topic类型，获取所有Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *BoltMQTopicService) GetTopicList() (map[models.TopicType][]string, error) {
	defer utils.RecoveredFn()
	params := make(map[models.TopicType][]string)
	topicList, err := service.GetAllList()
	if err != nil {
		return params, nil
	}
	topics := []string{}
	retryTopics := []string{}
	dlqTopics := []string{}
	for _, topic := range topicList {
		if models.IsRetryTopic(topic) {
			retryTopics = append(retryTopics, topic)
		} else if models.IsRetryTopic(topic) {
			dlqTopics = append(dlqTopics, topic)
		} else {
			topics = append(topics, topic)
		}
	}

	return params, nil
}

// Stats 分页查询Topic状态
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *BoltMQTopicService) GetTopicStats(topic string) ([]*models.TopicStats, error) {
	defer utils.RecoveredFn()
	topicStatsList := make([]*models.TopicStats, 0)

	service.InitMQAdmin()
	service.Start()
	defer service.Shutdown()

	topicStatsTable, err := service.DefaultMQAdminExtImpl.ExamineTopicStats(topic)
	if err != nil {
		return topicStatsList, err
	}
	if topicStatsTable == nil || topicStatsTable.OffsetTable == nil {
		return topicStatsList, nil
	}

	mqList := make(message.MessageQueues, 0, len(topicStatsTable.OffsetTable))
	for mq, _ := range topicStatsTable.OffsetTable {
		mqList = append(mqList, mq)
	}
	sort.Sort(mqList) // mqList 排序

	for _, mq := range mqList {
		if topicOffset, ok := topicStatsTable.OffsetTable[mq]; ok {
			topicStats := models.ToTopicStats(mq, topicOffset, mq.Topic, "")
			topicStatsList = append(topicStatsList, topicStats)
		}
	}

	return topicStatsList, nil
}

// UpdateTopicConfig 更新Topic配置信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *BoltMQTopicService) UpdateTopicConfig() error {
	return nil
}

// DeleteTopicFromCluster 删除指定集群对应broker所属的topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *BoltMQTopicService) DeleteTopic(topic, clusterName string) error {
	return nil
}

// CreateTopic 创建Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *BoltMQTopicService) CreateTopic(topic, clusterName string) error {
	return nil
}

// CreateTopic 创建Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *BoltMQTopicService) QueryTopicRoute(topic, clusterName string) (*route.TopicRouteData, error) {
	return nil, nil
}

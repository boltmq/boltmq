package topicService

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	"sort"
	"sync"
)

var (
	topicService *BoltMQTopicService
	sOnce        sync.Once
)

// BoltMQTopicService topic管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type BoltMQTopicService struct {
	*modules.AbstractService
}

// Default 返回默认唯一的用户处理对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func Default() *BoltMQTopicService {
	sOnce.Do(func() {
		topicService = NewBoltMQTopicService()
	})
	return topicService
}

// NewBoltMQTopicService 初始化Topic查询服务
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func NewBoltMQTopicService() *BoltMQTopicService {
	boltMQTopicService := &BoltMQTopicService{
		AbstractService: modules.NewAbstractService(),
	}
	return boltMQTopicService
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
		return []string{}, fmt.Errorf("DefaultMQAdminExtImpl FetchAllTopicList() is blank")
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

	params[models.NORMAL_TOPIC] = topics
	params[models.RETRY_TOPIC] = retryTopics
	params[models.DLQ_TOPIC] = dlqTopics
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
	defer utils.RecoveredFn()
	service.InitMQAdmin()
	service.Start()
	defer service.Shutdown()

	queueNum := 8
	err := service.DefaultMQAdminExtImpl.CreateTopic(stgcommon.DEFAULT_TOPIC, topic, queueNum)
	if err == nil {
		return nil
	}

	masterSet, err := service.DefaultMQAdminExtImpl.FetchMasterAddrByClusterName(clusterName)
	if err != nil {
		return err
	}
	if masterSet == nil || masterSet.Cardinality() == 0 {
		return fmt.Errorf("masterSet is empty, create topic failed. topic=%s, clusterName=%s", topic, clusterName)
	}

	perm := constant.PERM_READ | constant.PERM_WRITE
	for brokerAddr := range masterSet.Iterator().C {
		topicConfig := stgcommon.NewDefaultTopicConfig(topic, int32(queueNum), int32(queueNum), perm, stgcommon.SINGLE_TAG)
		err = service.DefaultMQAdminExtImpl.CreateCustomTopic(brokerAddr.(string), topicConfig)
		if err != nil {
			return fmt.Errorf("create topic err: %s, topic=%s", err.Error(), topic)
		}
	}

	return nil
}

// QueryTopicRoute 查询topic路由信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *BoltMQTopicService) QueryTopicRoute(topic, clusterName string) (*route.TopicRouteData, error) {
	return nil, nil
}

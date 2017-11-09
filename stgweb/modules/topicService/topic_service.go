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
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/clusterService"
	"sort"
	"strings"
	"sync"
)

var (
	topicService *TopicService
	sOnce        sync.Once
)

// TopicService topic管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type TopicService struct {
	*modules.AbstractService
	clusterService *clusterService.ClusterService
}

// Default 返回默认唯一的用户处理对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func Default() *TopicService {
	sOnce.Do(func() {
		topicService = NewTopicService()
		topicService.clusterService = clusterService.NewClusterService()
	})
	return topicService
}

// NewTopicService 初始化Topic查询服务
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func NewTopicService() *TopicService {
	topicServ := &TopicService{}
	topicServ.AbstractService = modules.Default()
	return topicServ
}

// List 查询所有Topic列表(不区分topic类型)
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *TopicService) GetAllList() (topicVos []*models.TopicVo, err error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	topicVos = make([]*models.TopicVo, 0)
	clusterTopicWappers, err := defaultMQAdminExt.GetClusterTopicWappers()
	if err != nil {
		return topicVos, err
	}
	if clusterTopicWappers == nil || len(clusterTopicWappers) == 0 {
		return topicVos, fmt.Errorf("DefaultMQAdminExtImpl GetClusterTopicWappers() is blank")
	}

	for _, wapper := range clusterTopicWappers {
		topicVo := models.ToTopicVo(wapper)
		topicVos = append(topicVos, topicVo)
	}
	logger.Infof("all topic size = %d", len(topicVos))
	return topicVos, nil
}

// GetTopicList 根据Topic类型，获取所有Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *TopicService) GetTopicList(clusterName, topicPrefix string, extra bool, topicType, limit, offset int) ([]*models.TopicVo, error) {
	defer utils.RecoveredFn()

	srcTopics, err := service.GetAllList()
	if err != nil {
		return []*models.TopicVo{}, nil
	}

	destTopics := service.GetTopicByParam(topicType, topicPrefix, srcTopics)
	topicVos := service.getTopicVoListByPaging(len(destTopics), limit, offset, destTopics)
	return topicVos, nil
}

// GetTopicByType 查询指定类型的topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/8
func (service *TopicService) GetTopicByParam(topicType int, prefix string, srcTopics []*models.TopicVo) (destTopics []*models.TopicVo) {
	// 查询所有Topic，不过滤
	destTopics = make([]*models.TopicVo, 0)
	if srcTopics == nil {
		return destTopics
	}
	if topicType == int(models.ALL_TOPIC) {
		for _, t := range srcTopics {
			if strings.EqualFold(prefix, "") || strings.HasPrefix(t.Topic, prefix) {
				destTopics = append(destTopics, t)
			}
		}
		return destTopics
	}

	for _, t := range srcTopics {
		tType := models.ParseTopicType(t.Topic)
		if (strings.EqualFold(prefix, "") || strings.HasPrefix(t.Topic, prefix)) && topicType == int(tType) {
			// 过滤指定类型的topic
			destTopics = append(destTopics, t)
		}
	}
	return destTopics
}

// getTopicVoListByPaging 分页获取TopicVo列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/7/14
func (service *TopicService) getTopicVoListByPaging(total, limit, offset int, topicVoList []*models.TopicVo) []*models.TopicVo {
	if total <= limit {
		// 数据条数 <= 前端每页显示的总条数，则直接返回，不做分页处理
		return topicVoList
	}

	// 处理分页
	var dataList []*models.TopicVo
	end := offset + limit - 1
	for i, t := range topicVoList {
		if i < offset {
			continue
		}
		dataList = append(dataList, t)
		if i >= end {
			break
		}
	}
	return dataList
}

// Stats 分页查询Topic状态
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *TopicService) GetTopicStats(topic string) ([]*models.TopicStats, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	topicStatsList := make([]*models.TopicStats, 0)
	clusterName, err := service.FindClusterByTopic(topic)
	if err != nil {
		return topicStatsList, err
	}

	topicStatsTable, err := defaultMQAdminExt.ExamineTopicStats(topic)
	if err != nil {
		return topicStatsList, err
	}
	if topicStatsTable == nil || topicStatsTable.OffsetTable == nil {
		return topicStatsList, nil
	}

	var mqList message.MessageQueues
	for mq, _ := range topicStatsTable.OffsetTable {
		mqList = append(mqList, mq)
	}
	sort.Sort(mqList) // mqList 排序

	for _, mq := range mqList {
		if topicOffset, ok := topicStatsTable.OffsetTable[mq]; ok {
			topicStats := models.ToTopicStats(mq, topicOffset, mq.Topic, clusterName)
			topicStatsList = append(topicStatsList, topicStats)
		}
	}

	return topicStatsList, nil
}

// UpdateTopicConfig 更新Topic配置信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *TopicService) UpdateTopicConfig() error {
	return nil
}

// DeleteTopicFromCluster 删除指定集群对应broker所属的topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *TopicService) DeleteTopic(topic, clusterName string) error {
	return nil
}

// CreateTopic 创建Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *TopicService) CreateTopic(topic, clusterName string) error {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	masterSet, err := defaultMQAdminExt.FetchMasterAddrByClusterName(clusterName)
	if err != nil {
		return err
	}
	if masterSet == nil || masterSet.Cardinality() == 0 {
		return fmt.Errorf("masterSet is empty, create topic failed. topic=%s, clusterName=%s", topic, clusterName)
	}

	queueNum := int32(8)
	perm := constant.PERM_READ | constant.PERM_WRITE
	for brokerAddr := range masterSet.Iterator().C {
		topicConfig := stgcommon.NewDefaultTopicConfig(topic, queueNum, queueNum, perm, stgcommon.SINGLE_TAG)
		err = defaultMQAdminExt.CreateCustomTopic(brokerAddr.(string), topicConfig)
		if err != nil {
			return fmt.Errorf("create topic err: %s, topic=%s", err.Error(), topic)
		}
	}

	return nil
}

// QueryTopicRoute 查询topic路由信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *TopicService) QueryTopicRoute(topic, clusterName string) (*route.TopicRouteData, error) {
	return nil, nil
}

// FindClusterByTopic 查询Topic归属的集群名称
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (service *TopicService) FindClusterByTopic(topic string) (string, error) {
	srcTopics, err := service.GetAllList()
	if err != nil {
		return "", err
	}
	if srcTopics == nil || len(srcTopics) == 0 {
		return "", fmt.Errorf("current topic[%s] do't has cluster", topic)
	}

	for _, t := range srcTopics {
		if t.Topic == topic {
			return t.ClusterName, nil
		}
	}

	return "", fmt.Errorf("find topic[%s] of clusterName failed", topic)
}

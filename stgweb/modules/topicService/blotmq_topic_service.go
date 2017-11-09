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
	topicService *BoltMQTopicService
	sOnce        sync.Once
)

// BoltMQTopicService topic管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type BoltMQTopicService struct {
	*modules.AbstractService
	clusterService *clusterService.ClusterService
}

// Default 返回默认唯一的用户处理对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func Default() *BoltMQTopicService {
	sOnce.Do(func() {
		topicService = NewTopicService()
		topicService.clusterService = clusterService.NewClusterService()
	})
	return topicService
}

// NewBoltMQTopicService 初始化Topic查询服务
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func NewTopicService() *BoltMQTopicService {
	boltMQTopicService := &BoltMQTopicService{
		AbstractService: modules.Default(),
	}
	return boltMQTopicService
}

// List 查询所有Topic列表(不区分topic类型)
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *BoltMQTopicService) GetAllList() (topicVos []*models.TopicVo, err error) {
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
func (service *BoltMQTopicService) GetTopicList(clusterName, topicPrefix string, extra bool, topicType, limit, offset int) ([]*models.TopicVo, error) {
	defer utils.RecoveredFn()

	srcTopics, err := service.GetAllList()
	if err != nil {
		return []*models.TopicVo{}, nil
	}

	topicVos := make([]*models.TopicVo, 0)
	destTopics := GetTopicByParam(topicType, topicPrefix, srcTopics)
	for _, t := range destTopics {
		topicVo := models.NewTopicVo(clusterName, t.Topic)
		topicVos = append(topicVos, topicVo)
	}

	topicVoList := getTopicVoListByPaging(len(topicVos), limit, offset, topicVos)
	return topicVoList, nil
}

// GetTopicByType 查询指定类型的topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/8
func GetTopicByParam(topicType int, prefix string, srcTopics []*models.TopicVo) (destTopics []*models.TopicVo) {
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
func getTopicVoListByPaging(total, limit, offset int, topicVoList []*models.TopicVo) (dataList []*models.TopicVo) {
	if total <= limit {
		// 数据条数 <= 前端每页显示的总条数，则直接返回，不做分页处理
		return topicVoList
	}

	// 处理分页
	dataList = make([]*models.TopicVo, 0)
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
func (service *BoltMQTopicService) GetTopicStats(topic string) ([]*models.TopicStats, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	topicStatsList := make([]*models.TopicStats, 0)
	topicStatsTable, err := defaultMQAdminExt.ExamineTopicStats(topic)
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
func (service *BoltMQTopicService) QueryTopicRoute(topic, clusterName string) (*route.TopicRouteData, error) {
	return nil, nil
}

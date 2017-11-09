package topicService

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/clusterService"
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
			return fmt.Errorf("create topic[%s] err: %s", topic, err.Error())
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
func (service *TopicService) FindClusterByTopic(topic string) (string, bool, error) {
	srcTopics, err := service.GetAllList()
	if err != nil {
		return "", false, err
	}
	if srcTopics == nil || len(srcTopics) == 0 {
		return "", false, fmt.Errorf("current topic[%s] do't has cluster", topic)
	}

	for _, t := range srcTopics {
		if t.Topic == topic {
			return t.ClusterName, true, nil
		}
	}

	return "", true, fmt.Errorf("topic[%s] not belong to one cluster", topic)
}

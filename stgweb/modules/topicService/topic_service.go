package topicService

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	set "github.com/deckarep/golang-set"
	"strings"
	"sync"
)

var (
	topicServ *TopicService
	sOnce     sync.Once
)

// TopicService topic管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type TopicService struct {
	*modules.AbstractService
}

// Default 返回默认唯一处理对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func Default() *TopicService {
	sOnce.Do(func() {
		topicServ = NewTopicService()
	})
	return topicServ
}

// NewTopicService 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func NewTopicService() *TopicService {
	return &TopicService{
		AbstractService: modules.Default(),
	}
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
func (service *TopicService) UpdateTopicConfig(topicVo *models.UpdateTopic) error {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	//if models.IsSystemTopic(topic) {
	//	return fmt.Errorf("系统Topic不允许更新")
	//}

	custername, _, err := service.FindClusterByTopic(topicVo.Topic)
	if err != nil || custername == "" {
		return fmt.Errorf("Topic名称 %s 不存在 ", topicVo.Topic)
	}

	masterSet, err := defaultMQAdminExt.FetchMasterAddrByClusterName(topicVo.ClusterName)
	if err != nil {
		return err
	}
	if masterSet == nil || masterSet.Cardinality() == 0 {
		return fmt.Errorf("masterSet is empty, update topic failed")
	}
	for brokerAddr := range masterSet.Iterator().C {
		defaultMQAdminExt.CreateAndUpdateTopicConfig(brokerAddr.(string), topicVo.ToTopicConfig())
	}
	return nil
}

// DeleteTopicFromCluster 删除指定集群对应broker所属的topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *TopicService) DeleteTopic(topic, clusterName string) error {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	if models.IsSystemTopic(topic) {
		return fmt.Errorf("系统Topic不允许删除")
	}

	custername, _, err := service.FindClusterByTopic(topic)
	if err != nil || custername == "" {
		return fmt.Errorf("Topic名称 %s 不存在 ", topic)
	}

	masterSet, err := defaultMQAdminExt.FetchMasterAddrByClusterName(clusterName)
	if err != nil {
		return err
	}
	if masterSet == nil || masterSet.Cardinality() == 0 {
		return fmt.Errorf("masterSet is empty, create topic failed. topic=%s, clusterName=%s", topic, clusterName)
	}
	logger.Infof("delete topic from broker. topic=%s, clusterName=%s, masterSet=%s", topic, clusterName, masterSet.String())
	defaultMQAdminExt.DeleteTopicInBroker(masterSet, topic)

	if !stgcommon.IsEmpty(service.ConfigureInitializer.GetNamesrvAddr()) {
		namesrvAddrs := strings.Split(service.ConfigureInitializer.GetNamesrvAddr(), ";")
		if namesrvAddrs != nil && len(namesrvAddrs) > 0 {
			nameServerSet := set.NewSet()
			for _, namesrvAddr := range namesrvAddrs {
				nameServerSet.Add(namesrvAddr)
			}
			logger.Infof("delete topic from namesrv. topic=%s, clusterName=%s, nameServerSet=%s", topic, clusterName, nameServerSet.String())
			defaultMQAdminExt.DeleteTopicInNameServer(nameServerSet, topic)
		}
	}
	return nil
}

// CreateTopic 创建Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *TopicService) CreateTopic(t *models.CreateTopic) error {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	masterSet, err := defaultMQAdminExt.FetchMasterAddrByClusterName(t.ClusterName)
	if err != nil {
		return err
	}
	if masterSet == nil || masterSet.Cardinality() == 0 {
		return fmt.Errorf("masterSet is empty, create topic failed. topic=%s, clusterName=%s", t.Topic, t.ClusterName)
	}
	for brokerAddr := range masterSet.Iterator().C {
		defaultMQAdminExt.CreateCustomTopic(brokerAddr.(string), t.ToTopicConfig())
	}
	return nil
}

// QueryTopicRoute 查询topic路由信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *TopicService) QueryTopicRoute(topic, clusterName string) (*route.TopicRouteData, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	topicRouteData, err := defaultMQAdminExt.ExamineTopicRouteInfo(topic)
	if err != nil || topicRouteData == nil {
		return route.NewTopicRouteData(), err
	}
	return topicRouteData, nil
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

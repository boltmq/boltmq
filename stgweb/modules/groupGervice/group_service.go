package groupGervice

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/topicService"
	"sync"
)

var (
	groupServ *GroupService
	sOnce     sync.Once
)

// GroupService 消费组、消费进度管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type GroupService struct {
	*modules.AbstractService
	TopicServ *topicService.TopicService
}

// Default 返回默认唯一处理对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func Default() *GroupService {
	sOnce.Do(func() {
		groupServ = NewGroupGervice()
	})
	return groupServ
}

// NewGroupGervice 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func NewGroupGervice() *GroupService {
	return &GroupService{
		AbstractService: modules.Default(),
		TopicServ:       topicService.Default(),
	}
}

// GroupList 查询所有topic对应的消费组列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/10
func (service *GroupService) GroupList(topic, clusterName string, limit, offset int) ([]*models.ConsumerGroupVo, int64, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	consumerGroupVos := make([]*models.ConsumerGroupVo, 0)
	total := int64(0)

	allTopic, err := service.TopicServ.GetAllList()
	if err != nil || allTopic == nil || len(allTopic) == 0 {
		return consumerGroupVos, total, err
	}

	for _, t := range allTopic {
		consumerGroupIds, err := service.QueryConsumerGroupId(t.Topic)
		if err != nil {
			return consumerGroupVos, total, err
		}
		for _, consumerGroupId := range consumerGroupIds {
			consumerGroupVo := models.NewConsumerGroupVo(t.ClusterName, t.Topic, consumerGroupId)
			consumerGroupVos = append(consumerGroupVos, consumerGroupVo)
		}
	}

	total = int64(len(consumerGroupVos))
	return service.consumerGroupPaging(total, limit, offset, consumerGroupVos), total, nil
}

// connectionOnlineListPaging 分页获取
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/7/14
func (service *GroupService) consumerGroupPaging(total int64, limit, offset int, list []*models.ConsumerGroupVo) []*models.ConsumerGroupVo {
	if total <= int64(limit) {
		// 数据条数 <= 前端每页显示的总条数，则直接返回，不做分页处理
		return list
	}

	// 处理分页
	var dataList []*models.ConsumerGroupVo
	end := offset + limit - 1
	for i, data := range list {
		if i < offset {
			continue
		}

		dataList = append(dataList, data)

		if i >= end {
			break
		}
	}
	return dataList
}

// GroupList 查询所有topic对应的消费组列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/10
func (service *GroupService) QueryConsumerGroupId(topic string) ([]string, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	consumerGroupIdList := make([]string, 0)
	groupList, err := defaultMQAdminExt.QueryTopicConsumeByWho(topic)
	if err != nil || groupList == nil || groupList.GroupList == nil || groupList.GroupList.Cardinality() == 0 {
		return consumerGroupIdList, err
	}

	for groupId := range groupList.GroupList.Iterator().C {
		consumerGroupIdList = append(consumerGroupIdList, groupId.(string))
	}
	return consumerGroupIdList, nil
}

// ConsumeProgress 查询消费进度
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/10
func (service *GroupService) ConsumeProgressByPage(topic, clusterName, consumerGroupId string, limit, offset int) (*models.ConsumerProgress, error) {

	return nil, nil
}

// ConsumeProgress 查询消费进度
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/10
func (service *GroupService) ConsumeProgress(topic, consumerGroupId string) (*models.ConsumerProgress, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	consumerProgress := new(models.ConsumerProgress)
	consumeTmpStats, err := defaultMQAdminExt.ExamineConsumeStatsByTopic(consumerGroupId, topic)
	if err != nil {
		return consumerProgress, err
	}

	if consumeTmpStats != nil {

	}
	return consumerProgress, nil
}

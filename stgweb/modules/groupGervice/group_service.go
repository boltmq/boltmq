package groupGervice

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/topicService"
	"sort"
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

	allTopic := make(models.TopicVos, 0)
	consumerGroupVos := make([]*models.ConsumerGroupVo, 0)
	total := int64(0)

	allTopic, err := service.TopicServ.GetAllList()
	if err != nil || allTopic == nil || len(allTopic) == 0 {
		return consumerGroupVos, total, err
	}
	sort.Sort(allTopic)

	for _, t := range allTopic {
		if topic != "" && t.Topic != topic {
			continue
		}

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
	progress, err := service.ConsumeProgress(topic, consumerGroupId)
	if err != nil {
		logger.Errorf("ConsumeProgressByPage err: %s", err.Error())
		data := make([]*models.ConsumerGroup, 0)
		return models.NewConsumerProgress(data, 0, 0, consumerGroupId, 0), nil
	}

	data := service.consumeProgressPaging(progress.Total, limit, offset, progress.Data)
	progress.Data = data
	return progress, nil
}

// connectionOnlineListPaging 分页获取
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/7/14
func (service *GroupService) consumeProgressPaging(total int64, limit, offset int, list []*models.ConsumerGroup) []*models.ConsumerGroup {
	if total <= int64(limit) {
		// 数据条数 <= 前端每页显示的总条数，则直接返回，不做分页处理
		return list
	}

	// 处理分页
	var dataList []*models.ConsumerGroup
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

// ConsumeProgress 查询消费进度
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/10
func (service *GroupService) ConsumeProgress(topic, consumerGroupId string) (*models.ConsumerProgress, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	consumerProgress := new(models.ConsumerProgress)
	consumeStats, err := defaultMQAdminExt.ExamineConsumeStatsByTopic(consumerGroupId, "")
	if err != nil {
		return consumerProgress, err
	}
	if consumeStats == nil || consumeStats.OffsetTable == nil || len(consumeStats.OffsetTable) == 0 {
		return consumerProgress, nil
	}

	mqList := make(message.MessageQueues, len(consumeStats.OffsetTable))
	index := 0
	for mq, _ := range consumeStats.OffsetTable {
		mqList[index] = mq
		index++
	}
	sort.Sort(mqList)

	var (
		groupVoList []*models.ConsumerGroup
		total       int64
		diffTotal   int64
		consumeTps  float64
	)
	for _, mq := range mqList {
		wapper, _ := consumeStats.OffsetTable[mq]
		groupVo, diff := models.ToConsumerGroup(mq, wapper)
		diffTotal += diff
		groupVoList = append(groupVoList, groupVo)
	}
	consumeTps = float64(consumeStats.ConsumeTps)
	total = int64(len(groupVoList))

	consumerProgress = models.NewConsumerProgress(groupVoList, total, diffTotal, consumerGroupId, consumeTps)
	return consumerProgress, nil
}

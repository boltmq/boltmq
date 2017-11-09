package topicService

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"sort"
)

// Stats 分页查询Topic状态
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *TopicService) GetTopicStats(topic string, limit, offset int) ([]*models.TopicStats, int64, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	topicStatsList := make([]*models.TopicStats, 0)
	clusterName, ok, err := service.FindClusterByTopic(topic)
	if err != nil {
		if ok {
			logger.Errorf(err.Error())
			return topicStatsList, 0, nil
		} else {
			// 使用 ok 额外标记：不存在的topic查询queue状态
			return topicStatsList, 0, err
		}
	}

	topicStatsTable, err := defaultMQAdminExt.ExamineTopicStats(topic)
	if err != nil {
		return topicStatsList, 0, err
	}
	if topicStatsTable == nil || topicStatsTable.OffsetTable == nil {
		return topicStatsList, 0, nil
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

	total := len(topicStatsList)
	return service.topicStatsPaging(total, limit, offset, topicStatsList), int64(total), nil
}

// topicStatsPaging 分页获取
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/7/14
func (service *TopicService) topicStatsPaging(total, limit, offset int, list []*models.TopicStats) []*models.TopicStats {
	if total <= limit {
		// 数据条数 <= 前端每页显示的总条数，则直接返回，不做分页处理
		return list
	}

	// 处理分页
	var dataList []*models.TopicStats
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

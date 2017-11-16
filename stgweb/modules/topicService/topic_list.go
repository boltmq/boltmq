package topicService

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"sort"
	"strings"
)

// GetTopicList 根据Topic类型，获取所有Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (service *TopicService) GetTopicList(clusterName, topicPrefix string, extra bool, topicType, limit, offset int) ([]*models.TopicVo, int64, error) {
	defer utils.RecoveredFn()

	srcTopics := make(models.TopicVos, 0)
	srcTopics, err := service.GetAllList()
	if err != nil {
		return srcTopics, 0, nil
	}

	sort.Sort(srcTopics)
	destTopics := service.GetTopicByParam(topicType, topicPrefix, srcTopics)
	total := len(destTopics)
	return service.topicVoListPaging(total, limit, offset, destTopics), int64(total), nil
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
			if prefix == "" || strings.Contains(t.Topic, prefix) {
				destTopics = append(destTopics, t)
			}
		}
		return destTopics
	}

	for _, t := range srcTopics {
		tType := models.ParseTopicType(t.Topic)
		if (prefix == "" || strings.Contains(t.Topic, prefix)) && topicType == int(tType) {
			// 过滤指定类型的topic
			destTopics = append(destTopics, t)
		}
	}
	return destTopics
}

// topicVoListPaging 分页获取
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/7/14
func (service *TopicService) topicVoListPaging(total, limit, offset int, list []*models.TopicVo) []*models.TopicVo {
	if total <= limit {
		// 数据条数 <= 前端每页显示的总条数，则直接返回，不做分页处理
		return list
	}

	// 处理分页
	var dataList []*models.TopicVo
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

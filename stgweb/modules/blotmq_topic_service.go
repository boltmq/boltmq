package modules

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"strings"
)

type BoltMQTopicService struct {
	AbstractService
}

func (service *BoltMQTopicService) list() (topics []string, err error) {
	service.BuildDefaultMQAdminExt()
	service.Start()
	topicList, err := service.DefaultMQAdminExt.FetchAllTopicList()
	if err != nil {
		logger.Errorf("defaultMQAdminExt.FetchAllTopicList() err: %s", err.Error())
		return []string{}, err
	}
	if topicList == nil || topicList.TopicList == nil || topicList.TopicList.Cardinality() == 0 {
		return []string{}, fmt.Errorf("defaultMQAdminExt.FetchAllTopicList() is blank.")
	}
	topics = make([]string, 0, topicList.TopicList.Cardinality())
	for topic := range topicList.TopicList.Iterator().C {
		topics = append(topics, topic.(string))
	}
	logger.Infof("all topic size = %d", len(topics))
	service.Shutdown()
	return topics, nil
}

func (service *BoltMQTopicService) getTopicList() (map[models.TopicType][]string, error) {
	params := make(map[models.TopicType][]string)
	topicList, err := service.list()
	if err != nil {
		return params, nil
	}
	topics := []string{}
	retryTopics := []string{}
	dlqTopics := []string{}
	for _, topic := range topicList {
		if strings.HasPrefix(topic, stgcommon.RETRY_GROUP_TOPIC_PREFIX) {
			retryTopics = append(retryTopics, topic)
		} else if strings.HasPrefix(topic, stgcommon.DLQ_GROUP_TOPIC_PREFIX) {
			dlqTopics = append(dlqTopics, topic)
		} else {
			topics = append(topics, topic)
		}
	}

	return params, nil
}

package modules

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
)

type BoltMQTopicService struct {
	AbstractService
}

func (service *BoltMQTopicService) list() (topics []string, err error) {
	defaultMQAdminExt := service.GetDefaultMQAdminExt()
	defaultMQAdminExt.Start()
	topicList, err := defaultMQAdminExt.FetchAllTopicList()
	if err != nil {
		logger.Errorf("defaultMQAdminExt.FetchAllTopicList() err: %s", err.Error())
		return []string{}, err
	}
	if topicList == nil || topicList.TopicList == nil || topicList.TopicList.Cardinality() == 0 {
		return []string{}, fmt.Errorf("defaultMQAdminExt.FetchAllTopicList() is blank.")
	}
	topics = make([]string, 0, topicList.TopicList.Cardinality())
	for topic := range topicList.TopicList.Iterator().C {
		topics = append(topics, topic)
	}
	logger.Infof("all topic size = %d", len(topics))
	service.Shutdown(defaultMQAdminExt)
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
	for _, t := range topicList {
		if(topic.startsWith("%RETRY%")){
			retryTopics.add(topic);
		} else if(topic.startsWith("%DLQ%")) {
			dlqTopics.add(topic);
		} else {
			topics.add(topic);
		}
	}

	return params, nil
}

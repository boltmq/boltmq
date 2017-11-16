package connectionService

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/groupGervice"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/topicService"
	"sort"
	"strings"
	"sync"
)

var (
	clusterService *ConnectionService
	sOnce          sync.Once
)

// ConnectionService 在线进程Connection管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ConnectionService struct {
	*modules.AbstractService
	TopicServ *topicService.TopicService
	GroupServ *groupGervice.GroupService
}

// Default 返回默认唯一处理对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func Default() *ConnectionService {
	sOnce.Do(func() {
		clusterService = NewConnectionService()
	})
	return clusterService
}

// NewConnectionService 初始化Topic查询服务
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func NewConnectionService() *ConnectionService {
	return &ConnectionService{
		AbstractService: modules.Default(),
		TopicServ:       topicService.Default(),
		GroupServ:       groupGervice.Default(),
	}
}

// ConnectionOnline
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/10
func (service *ConnectionService) ConnectionOnline(searchTopic string, limit, offset int) ([]*models.ConnectionOnline, int64, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	var (
		connectionOnlines models.ConnectionOnlines
		total             int64
	)

	allTopic, err := service.TopicServ.GetAllList()
	if err != nil || allTopic == nil || len(allTopic) == 0 {
		return connectionOnlines, total, err
	}

	for _, t := range allTopic {
		if searchTopic != "" && !strings.Contains(t.Topic, searchTopic) {
			// logger.Warnf("search topic [%s] is invalid", t.Topic)
			continue
		}
		consumerGroupIds, consumerNums, err := service.SumOnlineConsumerNums(t.Topic)
		if err != nil {
			return connectionOnlines, total, err
		}

		_, producerNums, err := service.sumOnlineProducerNums(t.Topic)
		if err != nil {
			return connectionOnlines, total, err
		}

		connectionOnline := models.NewConnectionOnline(t.ClusterName, t.Topic, consumerGroupIds, consumerNums, producerNums)
		connectionOnlines = append(connectionOnlines, connectionOnline)
	}

	sort.Sort(connectionOnlines)
	total = int64(len(connectionOnlines))
	pageConnections := service.connectionOnlineListPaging(total, limit, offset, connectionOnlines)
	return pageConnections, total, nil
}

// sumOnlineProducerNums 统计topic对应的在线生产进程数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/10
func (service *ConnectionService) sumOnlineProducerNums(topic string) ([]string, int, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	var (
		producerGroupIds []string
		producerNums     int
	)

	//defaultMQAdminExt.ExamineProducerConnectionInfo("", topic)
	return producerGroupIds, producerNums, nil
}

// SumOnlineConsumerNums 统计topic对应的在线生产进程数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/10
func (service *ConnectionService) SumOnlineConsumerNums(topic string) ([]string, int, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	consumerGroupIds := make([]string, 0)
	consumerNums := 0
	groupList, err := defaultMQAdminExt.QueryTopicConsumeByWho(topic)
	if err != nil {
		return consumerGroupIds, consumerNums, err
	}
	if groupList == nil || groupList.GroupList == nil || groupList.GroupList.Cardinality() == 0 {
		return consumerGroupIds, consumerNums, nil
	}

	for itor := range groupList.GroupList.Iterator().C {
		if groupId, ok := itor.(string); ok {
			consumerConnection, err := defaultMQAdminExt.ExamineConsumerConnectionInfo(groupId, topic)

			if err != nil {
				logger.Errorf("query consumerConnection err: %s.  consumerGroupId=%s", err.Error(), groupId)
				// return consumerGroupIdList, consumerNums, err // ingore
			} else {
				if consumerConnection != nil && consumerConnection.ConnectionSet != nil {
					consumerGroupIds = append(consumerGroupIds, groupId)
					consumerNums += len(consumerConnection.ConnectionSet)
				}
			}
		}
	}
	logger.Infof("sumOnlineConsumerNums.consumerGroupIds = [%v] ", strings.Join(consumerGroupIds, ","))
	return consumerGroupIds, consumerNums, nil
}

// connectionOnlineListPaging 分页获取
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/7/14
func (service *ConnectionService) connectionOnlineListPaging(total int64, limit, offset int, list []*models.ConnectionOnline) []*models.ConnectionOnline {
	if total <= int64(limit) {
		// 数据条数 <= 前端每页显示的总条数，则直接返回，不做分页处理
		return list
	}

	// 处理分页
	var dataList []*models.ConnectionOnline
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

// ConnectionDetail 查询在线消费进程、在线生产进程的详情
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/10
func (service *ConnectionService) ConnectionDetail(clusterName, searchTopic string) (*models.ConnectionDetail, error) {

	connectionDetail := new(models.ConnectionDetail)
	connectionDetail.ConsumerOnLine = new(models.ConsumerOnLine)
	consumerConnectionVos, err := service.queryOnlineConsumer(clusterName, searchTopic)
	if err != nil {
		connectionDetail.ConsumerOnLine.Describe = err.Error()
	}
	connectionDetail.ConsumerOnLine.Connection = consumerConnectionVos
	connectionDetail.ConsumerOnLine.Topic = searchTopic
	connectionDetail.ConsumerOnLine.ClusterName = clusterName
	return connectionDetail, nil
}

// queryOnlineConsumer 查询在线消费进程详情
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/10
func (service *ConnectionService) queryOnlineConsumer(clusterName, topic string) ([]*models.ConsumerConnectionVo, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	result := make([]*models.ConsumerConnectionVo, 0)

	groupList, err := defaultMQAdminExt.QueryTopicConsumeByWho(topic)
	if err != nil {
		return result, err
	}
	if groupList == nil || groupList.GroupList == nil || groupList.GroupList.Cardinality() == 0 {
		return result, nil
	}

	for itor := range groupList.GroupList.Iterator().C {
		if groupId, ok := itor.(string); ok {
			cc, err := defaultMQAdminExt.ExamineConsumerConnectionInfo(groupId, topic)
			if err != nil {
				logger.Errorf("query consumerConnection error: %s. consumerGroupId=%s, topic=%s", err.Error(), groupId, topic)
				return result, err
			}

			progress, err := service.GroupServ.ConsumeProgress(topic, groupId)
			if err != nil {
				logger.Errorf("query consumerProgress err: %s. consumerGroupId=%s, topic=%s", err.Error(), groupId, topic)
				return result, err
			}

			if cc != nil && cc.ConnectionSet != nil {
				for _, c := range cc.ConnectionSet {
					if c == nil {
						continue
					}
					consumerConnectionVo := models.ToConsumerConnectionVo(c, cc, progress, groupId)
					result = append(result, consumerConnectionVo)
				}
			}
		}
	}
	return result, nil
}

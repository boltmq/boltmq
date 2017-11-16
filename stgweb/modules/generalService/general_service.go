package generalService

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/brokerService"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/connectionService"
	set "github.com/deckarep/golang-set"
	"strings"
	"sync"
)

var (
	generalServ *GeneralService
	sOnce       sync.Once
)

// GeneralService 首页概览处理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type GeneralService struct {
	*modules.AbstractService
	BrokerServ     *brokerService.BrokerService
	ConnectionServ *connectionService.ConnectionService
}

// Default 返回默认唯一处理对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func Default() *GeneralService {
	sOnce.Do(func() {
		generalServ = NewGeneralService()
	})
	return generalServ
}

// NewGeneralService 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func NewGeneralService() *GeneralService {
	return &GeneralService{
		AbstractService: modules.Default(),
		BrokerServ:      brokerService.Default(),
		ConnectionServ:  connectionService.Default(),
	}
}

// GeneralStats 查询首页概览的统计数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func (service *GeneralService) GeneralStats() (*models.GeneralVo, error) {
	// TODO: 后续考虑使用定时器统计数据
	return service.CalculationGeneralStats()
}

// getNamesrvCount 查询namesrv节点个数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func (service *GeneralService) getNamesrvCount() (int64, error) {
	namesrvAddr := service.ConfigureInitializer.GetNamesrvAddr()
	namesrvAddrNodes := strings.Split(namesrvAddr, ";")
	return int64(len(namesrvAddrNodes)), nil
}

// getClusterAndBrokerCount 查询cluster集群个数、broker组数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func (service *GeneralService) getClusterAndBrokerCount() (int64, int64, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	clusterCount := int64(0)
	brokerCount := int64(0)
	clusterNames, brokerAddrTable, err := defaultMQAdminExt.GetAllClusterNames()
	if err != nil {
		return clusterCount, brokerCount, err
	}
	clusterCount = int64(len(clusterNames))

	if brokerAddrTable != nil && len(brokerAddrTable) > 0 {
		for _, brokerData := range brokerAddrTable {
			if brokerData != nil && brokerData.BrokerAddrs != nil {
				brokerCount += int64(len(brokerData.BrokerAddrs))
			}
		}
	}

	return clusterCount, brokerCount, nil
}

// getTopicCount 查询topic个数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func (service *GeneralService) getTopicCount() (int64, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	topicCount := int64(0)
	topicWapper, err := defaultMQAdminExt.GetClusterTopicWappers()
	if err != nil {
		return topicCount, err
	}
	topicCount = int64(len(topicWapper))
	return topicCount, nil
}

// getMessageCount 查询消息条数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func (service *GeneralService) getMessageCount() (*models.GeneralStats, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	generalStats := new(models.GeneralStats)
	brokerStats, err := service.BrokerServ.GetBrokerRuntimeInfo()
	if err != nil {
		return generalStats, err
	}
	if brokerStats == nil || brokerStats.ClusterGeneralVo == nil {
		return generalStats, fmt.Errorf(" brokerStats.ClusterGeneralVo  is nil")
	}

	var (
		outTotalTodayAll int64
		outTotalYestAll  int64
		inTotalYestAll   int64
		inTotalTodayAll  int64
	)

	for _, cgv := range brokerStats.ClusterGeneralVo {
		if cgv == nil {
			continue
		}
		for _, bg := range cgv.BrokerGeneral {
			outTotalTodayAll += bg.OutTotalToday
			outTotalYestAll += bg.OutTotalYest
			inTotalYestAll += bg.InTotalYest
			inTotalTodayAll += bg.InTotalToday
		}
	}

	generalStats = models.NewGeneralStats(outTotalTodayAll, outTotalYestAll, inTotalYestAll, inTotalTodayAll)
	return generalStats, nil
}

// getConsumerCount 统计在线conusmer进程个数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func (service *GeneralService) getConsumerCount() (int64, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	conusmerCount := int64(0)
	topicWapper, err := defaultMQAdminExt.GetClusterTopicWappers()
	if err != nil {
		return conusmerCount, err
	}

	groups := set.NewSet()
	for _, tw := range topicWapper {
		if models.IsRetryTopic(tw.TopicName) {
			continue // 过滤重试TTopic对应的数据
		}
		groupSet, _, err := service.ConnectionServ.SumOnlineConsumerNums(tw.TopicName)
		if err != nil {
			continue
		}
		groups = groups.Union(groupSet)
	}
	conusmerCount += int64(groups.Cardinality())
	return conusmerCount, nil
}

// getProducerCount 统计在线producer进程个数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func (service *GeneralService) getProducerCount() (int64, error) {
	// 无法直接根据topic查询在线producer进程个数，后台处理默认值，便于页面展示
	producerCount := int64(2)
	return producerCount, nil
}

// CalculationGeneralStats 统计首页概览数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func (service *GeneralService) CalculationGeneralStats() (*models.GeneralVo, error) {
	generalVo := new(models.GeneralVo)

	producerCount, err := service.getProducerCount()
	if err != nil {
		logger.Errorf("getProducerCount err: %s", err.Error())
	}

	consumerCount, err := service.getConsumerCount()
	if err != nil {
		logger.Errorf("getConsumerCount err: %s", err.Error())
	}

	clusterCount, brokerCount, err := service.getClusterAndBrokerCount()
	if err != nil {
		logger.Errorf("getClusterAndBrokerCount err: %s", err.Error())
	}

	namesrvCount, err := service.getNamesrvCount()
	if err != nil {
		logger.Errorf("getNamesrvCount err: %s", err.Error())
	}

	topicCount, err := service.getTopicCount()
	if err != nil {
		logger.Errorf("getTopicCount err: %s", err.Error())
	}

	generalNode := models.NewGeneralNode(producerCount, consumerCount, clusterCount, brokerCount, namesrvCount, topicCount)
	generalVo.GeneralNode = generalNode

	generalStats, err := service.getMessageCount()
	if err != nil {
		logger.Errorf("getMessageCount err: %s", err.Error())
	}
	generalVo.GeneralStats = generalStats

	return generalVo, nil
}

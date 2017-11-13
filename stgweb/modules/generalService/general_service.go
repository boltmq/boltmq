package generalService

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/brokerService"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/topicService"
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
	TopicServ  *topicService.TopicService
	BrokerServ *brokerService.BrokerService
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
		TopicServ:       topicService.Default(),
		BrokerServ:      brokerService.Default(),
	}
}

// GeneralStats 查询首页概览的统计数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func (service *GeneralService) GeneralStats() (*models.GeneralStats, error) {
	return nil, nil
}

// getNamesrvCount 查询namesrv节点个数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func (service *GeneralService) getNamesrvCount() (int64, error) {
	namesrvAddr := service.ConfigureInitializer.GetNamesrvAddr()
	namesrvAddrNodes := strings.Split(namesrvAddr, ";")
	return int64(len(namesrvAddrNodes)), nil
}

// getBrokerCount 查询broker组数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func (service *GeneralService) getBrokerCount() (int64, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	return 0, nil
}

// getBrokerCount 查询broker组数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func (service *GeneralService) getClusterCount() (int64, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	defaultMQAdminExt.GetAllClusterNames()
	return 0, nil
}

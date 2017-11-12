package clusterService

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	"sync"
)

var (
	clusterServ *ClusterService
	sOnce       sync.Once
)

// ClusterService 集群Cluster管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ClusterService struct {
	*modules.AbstractService
}

// Default 返回默认唯一处理对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func Default() *ClusterService {
	sOnce.Do(func() {
		clusterServ = NewClusterService()
	})
	return clusterServ
}

// NewClusterService 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func NewClusterService() *ClusterService {
	return &ClusterService{
		AbstractService: modules.Default(),
	}
}

// GetCluserNames 查询所有集群名称
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/8
func (service *ClusterService) GetCluserNames() (*models.ClusterList, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	clusterList := &models.ClusterList{}
	clusterNames, _, err := defaultMQAdminExt.GetAllClusterNames()
	if err != nil {
		return clusterList, err
	}

	clusterList.ClusterNames = clusterNames
	return clusterList, nil
}

// GetNamesrvNodes 获取namesrv节点列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (service *ClusterService) GetNamesrvNodes() ([]string, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	namesrvList, err := defaultMQAdminExt.GetNameServerAddressList()
	if err != nil {
		return []string{}, err
	}
	return namesrvList, nil
}

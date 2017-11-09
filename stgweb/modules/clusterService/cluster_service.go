package clusterService

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	"sync"
)

var (
	clusterService *ClusterService
	sOnce          sync.Once
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
		clusterService = NewClusterService()
	})
	return clusterService
}

// NewClusterService 初始化Topic查询服务
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func NewClusterService() *ClusterService {
	boltMQClusterService := &ClusterService{
		AbstractService: modules.Default(),
	}
	return boltMQClusterService
}

// GetCluserNames 查询所有集群名称
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/8
func (service *ClusterService) GetCluserNames() ([]string, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	clusterNames, _, err := defaultMQAdminExt.GetAllClusterNames()
	if err != nil {
		return []string{}, err
	}

	return clusterNames, nil
}

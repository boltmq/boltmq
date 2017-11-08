package clusterService

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	"sync"
)

var (
	clusterService *BoltMQClusterService
	sOnce          sync.Once
)

// BoltMQClusterService 集群Cluster管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type BoltMQClusterService struct {
	*modules.AbstractService
}

// Default 返回默认唯一的用户处理对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func Default() *BoltMQClusterService {
	sOnce.Do(func() {
		clusterService = NewBoltMQClusterService()
	})
	return clusterService
}

// NewBoltMQTopicService 初始化Topic查询服务
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func NewBoltMQClusterService() *BoltMQClusterService {
	boltMQClusterService := &BoltMQClusterService{
		AbstractService: modules.NewAbstractService(),
	}
	return boltMQClusterService
}

// GetCluserNames 查询所有集群名称
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/8
func (service *BoltMQClusterService) GetCluserNames() ([]string, error) {
	defer utils.RecoveredFn()
	service.InitMQAdmin()
	service.Start()
	defer service.Shutdown()

	clusterNames, _, err := service.DefaultMQAdminExtImpl.GetAllClusterNames()
	if err != nil {
		return []string{}, err
	}

	return clusterNames
}

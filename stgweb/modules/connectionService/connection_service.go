package connectionService

import (
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
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
	}
}

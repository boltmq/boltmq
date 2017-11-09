package generalService

import (
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
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
	}
}

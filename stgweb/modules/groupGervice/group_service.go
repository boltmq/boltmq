package groupGervice

import (
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	"sync"
)

var (
	groupServ *GroupGervice
	sOnce     sync.Once
)

// GroupGervice 消费组、消费进度管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type GroupGervice struct {
	*modules.AbstractService
}

// Default 返回默认唯一处理对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func Default() *GroupGervice {
	sOnce.Do(func() {
		groupServ = NewGroupGervice()
	})
	return groupServ
}

// NewGroupGervice 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func NewGroupGervice() *GroupGervice {
	return &GroupGervice{
		AbstractService: modules.Default(),
	}
}

package processor

import (
	"git.oschina.net/cloudzone/smartgo/stgregistry/controller"
)

var (
	namesrvController *controller.NamesrvController
)

// DefaultRequestProcessor NameServer网络请求处理结构体
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
type DefaultRequestProcessor struct {
	NamesrvController *controller.NamesrvController
}

// NewDefaultRequestProcessor 初始化NameServer网络请求处理
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func NewDefaultRequestProcessor(namesrvContro *controller.NamesrvController) *DefaultRequestProcessor {
	defaultRequestProcessor := DefaultRequestProcessor{
		NamesrvController: namesrvContro,
	}

	namesrvController = namesrvContro
	return &defaultRequestProcessor
}

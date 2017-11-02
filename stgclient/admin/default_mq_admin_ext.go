package admin

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
)

// DefaultMQAdminExt 所有运维接口都在这里实现
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
type DefaultMQAdminExt struct {
	CreateTopicKey string
	AdminExtGroup  string
	DefaultMQAdminExtImpl
}

func NewDefaultMQAdminExt() *DefaultMQAdminExt {
	defaultMQAdminExt := &DefaultMQAdminExt{}
	defaultMQAdminExt.AdminExtGroup = "admin_ext_group"
	defaultMQAdminExt.CreateTopicKey = stgcommon.DEFAULT_TOPIC
	return defaultMQAdminExt
}

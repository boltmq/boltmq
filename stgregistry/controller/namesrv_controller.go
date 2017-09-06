package controller

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	//"git.oschina.net/cloudzone/smartgo/stgregistry/kvconfig"
	//"git.oschina.net/cloudzone/smartgo/stgregistry/routeinfo"
)

// NamesrvController 注意循环引用
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
type NamesrvController struct {
	NamesrvConfig *namesrv.NamesrvConfig
	//RouteInfoManager routeinfo.RouteInfoManager
	//KvConfigManager  kvconfig.KVConfigSerializeWrapper
}

func (self *NamesrvController) shutdown() {

}

func start() error {
	return nil
}

func registerProcessor() {

}

func initialize() bool {
	return false
}

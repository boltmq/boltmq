package controller

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
)

// NamesrvController 注意循环引用
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
type NamesrvController struct {
	NamesrvConfig  *namesrv.NamesrvConfig
	RemotingServer *remoting.RemotingServer
	//RouteInfoManager routeinfo.RouteInfoManager
	//KvConfigManager  kvconfig.KVConfigSerializeWrapper
}

func (self *NamesrvController) shutdown() {
	//RemotingServer := remoting.NewDefalutRemotingClient()

	//defaultRequestProcessor := processor.NewDefaultRequestProcessor(self)
	//RemotingServer.RegisterDefaultProcessor(nil)
}

func (self *NamesrvController) start() error {
	return nil
}

func (self *NamesrvController) registerProcessor() {

}

func (self *NamesrvController) initialize() bool {
	return false
}

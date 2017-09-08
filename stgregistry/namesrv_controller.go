package stgregistry

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
)

// DefaultNamesrvController 注意循环引用
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
type DefaultNamesrvController struct {
	NamesrvConfig    *namesrv.NamesrvConfig
	RemotingServer   *remoting.RemotingServer
	RouteInfoManager *RouteInfoManager
	KvConfigManager  *KVConfigSerializeWrapper
}

// NamesrvController
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
type NamesrvController interface {
}

func (self *DefaultNamesrvController) shutdown() {
	//RemotingServer := remoting.NewDefalutRemotingClient()

	//defaultRequestProcessor := processor.NewDefaultRequestProcessor(self)
	//RemotingServer.RegisterDefaultProcessor(nil)
}

func (self *DefaultNamesrvController) start() error {
	return nil
}

func (self *DefaultNamesrvController) registerProcessor() {

}

func (self *DefaultNamesrvController) initialize() bool {
	return false
}

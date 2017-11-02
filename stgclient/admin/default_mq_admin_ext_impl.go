package admin

import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
)

// DefaultMQAdminExtImpl 所有运维接口都在这里实现
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/2
type DefaultMQAdminExtImpl struct {
	ServiceState     stgcommon.ServiceState
	MqClientInstance process.MQClientInstance
	RpcHook          remoting.RPCHook
	ClientConfig     *stgclient.ClientConfig
	MQAdminExtInner
	stgclient.MQAdmin
}

func NewDefaultMQAdminExtImpl() *DefaultMQAdminExtImpl {
	defaultMQAdminExtImpl := &DefaultMQAdminExtImpl{}
	return defaultMQAdminExtImpl
}

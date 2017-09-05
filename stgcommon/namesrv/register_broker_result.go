package namesrv

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
)

// RegisterBrokerResult 注册broker返回结果
// Author gaoyanlei
// Since 2017/8/23
type RegisterBrokerResult struct {
	HaServerAddr string
	MasterAddr   string
	KvTable      body.KVTable
}

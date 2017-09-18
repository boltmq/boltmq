package body

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// SubscriptionGroupWrapper 订阅组配置，序列化包装
// Author gaoyanlei
// Since 2017/8/22
type SubscriptionGroupWrapper struct {
	subscriptionGroupTable *sync.Map
	dataVersion            stgcommon.DataVersion
	*protocol.RemotingSerializable
}

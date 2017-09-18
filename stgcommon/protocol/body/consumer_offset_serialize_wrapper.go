package body

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// ConsumerOffsetSerializeWrapper Consumer消费进度，序列化包装
// Author gaoyanlei
// Since 2017/8/22
type ConsumerOffsetSerializeWrapper struct {
	offsetTable *sync.Map
	*protocol.RemotingSerializable
}


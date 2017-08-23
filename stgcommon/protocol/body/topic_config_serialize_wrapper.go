package body

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// TopicConfigSerializeWrapper topic
// @author gaoyanlei
// @since 2017/8/11
type TopicConfigSerializeWrapper struct {
	TopicConfigs     []stgcommon.TopicConfig `json:"topicConfigTable"`
	TopicConfigTable *sync.Map
	DataVersion      *stgcommon.DataVersion  `json:"dataVersion"`
	*protocol.RemotingSerializable
}


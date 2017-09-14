package body

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// TopicConfigSerializeWrapper topic
// @author gaoyanlei
// @since 2017/8/11
type TopicConfigSerializeWrapper struct {
	TopicConfigTable *TopicConfigTable      `json:"topiConfigTable"`
	DataVersion      *stgcommon.DataVersion `json:"dataVersion"`
	*protocol.RemotingSerializable
}

func NewTopicConfigSerializeWrapper() *TopicConfigSerializeWrapper {
	return &TopicConfigSerializeWrapper{
		TopicConfigTable: NewTopicConfigTable(),
		DataVersion:      stgcommon.NewDataVersion(),
	}
}

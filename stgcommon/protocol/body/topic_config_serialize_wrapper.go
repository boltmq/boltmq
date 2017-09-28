package body

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// TopicConfigSerializeWrapper topic
// @author gaoyanlei
// @since 2017/8/11
type TopicConfigSerializeWrapper struct {
	TopicConfigTable *TopicConfigTable      `json:"topicConfigTable"`
	DataVersion      *stgcommon.DataVersion `json:"dataVersion"`
	*protocol.RemotingSerializable
}

func NewTopicConfigSerializeWrapper(dataVersion ...*stgcommon.DataVersion) *TopicConfigSerializeWrapper {
	topicConfigSerializeWrapper := &TopicConfigSerializeWrapper{
		TopicConfigTable:     NewTopicConfigTable(),
		RemotingSerializable: new(protocol.RemotingSerializable),
	}

	topicConfigSerializeWrapper.DataVersion = stgcommon.NewDataVersion()
	if dataVersion != nil && len(dataVersion) > 0 {
		topicConfigSerializeWrapper.DataVersion = dataVersion[0]
	}
	return topicConfigSerializeWrapper
}

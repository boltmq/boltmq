package body

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
)

// TopicConfigSerializeWrapper topic
// @author gaoyanlei
// @since 2017/8/11
type TopicConfigSerializeWrapper struct {
	TopicConfigs     []stgcommon.TopicConfig `json:"topicConfigTable"`
	TopicConfigTable *sync.Map
	DataVersion      *stgcommon.DataVersion  `json:"dataVersion"`
}

// 初始化 TopicConfigSerializeWrapper
// @author gaoyanlei
// @since 2017/8/11
func NewTopicConfigSerializeWrapper() *TopicConfigSerializeWrapper {
	return &TopicConfigSerializeWrapper{
		TopicConfigTable: sync.NewMap(),
		DataVersion:      stgcommon.NewDataVersion(),
	}
}

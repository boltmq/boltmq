package body

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
)

type RegisterBrokerBody struct {
	TopicConfigSerializeWrapper TopicConfigSerializeWrapper
	FilterServerList []string
}

func NewTopicConfigSerializeWrapper() *TopicConfigSerializeWrapper {
	return &TopicConfigSerializeWrapper{
		TopicConfigTable: sync.NewMap(),
		DataVersion:      stgcommon.NewDataVersion(),
	}
}

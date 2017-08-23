package body



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

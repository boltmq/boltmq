package body

type RegisterBrokerBody struct {
	TopicConfigSerializeWrapper *TopicConfigSerializeWrapper
	FilterServerList []string
}


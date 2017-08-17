package message

// MessageAccessor: 消息
// Author: yintongqiang
// Since:  2017/8/16

func GetReconsumeTime(msg Message) string {
	return msg.Properties[PROPERTY_RECONSUME_TIME]
}

func ClearProperty(msg *Message, name string) {
	msg.ClearProperty(name)
}

func PutProperty(msg *Message, name string, value string) {
	msg.PutProperty(name, value)
}

func SetProperties(msg *Message, name string, value string) {
	msg.PutProperty(name, value)
}

func SetPropertiesMap(msg *Message, properties map[string]string) {
	msg.Properties = properties
}

func GetOriginMessageId(msg Message) string {
	return msg.Properties[PROPERTY_ORIGIN_MESSAGE_ID]
}

func SetOriginMessageId(msg *Message, originMessageId string) {
	PutProperty(msg, PROPERTY_ORIGIN_MESSAGE_ID, originMessageId)
}

func SetReconsumeTime(msg *Message, reconsumeTimes string) {
	PutProperty(msg, PROPERTY_RECONSUME_TIME, reconsumeTimes)
}

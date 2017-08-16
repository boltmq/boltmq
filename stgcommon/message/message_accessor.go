package message
// MessageAccessor: 消息
// Author: yintongqiang
// Since:  2017/8/16

func GetReconsumeTime(msg Message) string {
	return msg.Properties[PROPERTY_RECONSUME_TIME]
}

func ClearProperty(msg *Message, name string) {
	msg.clearProperty(name)
}

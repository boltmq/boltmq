package message

// Message: 消息结构体
// Author: yintongqiang
// Since:  2017/8/9


type Message struct {
	// 消息主题
	Topic      string
	// 消息标志，系统不做干预，完全由应用决定如何使用
	Flag       int
	// 消息属性，都是系统属性，禁止应用设置
	Properties map[string]string
	// 消息体
	Body       []byte
}

func NewMessage(topic string, tags string, body[]byte) Message {
	properties := make(map[string]string)
	properties[PROPERTY_TAGS] = tags
	return Message{Topic:topic, Properties:properties, Body:body}
}

func (msg *Message)clearProperty(name string) {
	delete(msg.Properties, name)
}
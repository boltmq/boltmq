package message

import "strconv"

// Message: 消息结构体
// Author: yintongqiang
// Since:  2017/8/9

type Message struct {
	// 消息主题
	Topic string
	// 消息标志，系统不做干预，完全由应用决定如何使用
	Flag int32
	// 消息属性，都是系统属性，禁止应用设置
	Properties map[string]string
	// 消息体
	Body []byte
}

func NewMessage(topic string, tags string, body []byte) *Message {
	properties := make(map[string]string)
	properties[PROPERTY_TAGS] = tags
	return &Message{Topic: topic, Properties: properties, Body: body}
}

func (msg *Message) ClearProperty(name string) {
	delete(msg.Properties, name)
}

func (self *Message) PutProperty(name string, value string) {
	if self.Properties == nil {
		self.Properties = make(map[string]string)
	}

	self.Properties[name] = value
}

func (self *Message) GetProperty(name string) string {
	if self.Properties == nil {
		self.Properties = make(map[string]string)
	}
	return self.Properties[name]
}

func (self *Message) GetTags() string {
	return self.GetProperty(PROPERTY_TAGS)
}

func (self *Message) SetWaitStoreMsgOK(waitStoreMsgOK bool) {
	self.PutProperty(PROPERTY_WAIT_STORE_MSG_OK, strconv.FormatBool(waitStoreMsgOK))
}

func (self *Message) SetDelayTimeLevel(level int) {
	self.PutProperty(PROPERTY_DELAY_TIME_LEVEL, strconv.Itoa(level))
}

func (self *Message) GetKeys() string {
	return self.GetProperty(PROPERTY_KEYS)
}

func (self *Message) SetKeys(keys string) {
	self.PutProperty(PROPERTY_KEYS, keys)
}

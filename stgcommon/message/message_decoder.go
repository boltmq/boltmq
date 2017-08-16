package message

import (
	"bytes"
)

// MessageDecoder: 消息解码
// Author: yintongqiang
// Since:  2017/8/16
const (
	// 消息ID定长
	MSG_ID_LENGTH = 8 + 8


	// 存储记录各个字段位置
	MessageMagicCodePostion = 4
	MessageFlagPostion = 16
	MessagePhysicOffsetPostion = 28
	MessageStoreTimestampPostion = 56
	MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8
	charset = "utf-8"
	// 序列化消息属性
	NAME_VALUE_SEPARATOR = 1
	PROPERTY_SEPARATOR = 2
)

func MessageProperties2String(properties map[string]string) string {
	b := bytes.Buffer{}
	for k, v := range properties {
		b.WriteString(k)
		b.WriteString(string(NAME_VALUE_SEPARATOR))
		b.WriteString(v)
		b.WriteString(string(PROPERTY_SEPARATOR))
	}
	return b.String()
}
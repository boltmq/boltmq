package message

import (
	"bytes"
	"golang.org/x/tools/go/gcimporter15/testdata"
	"strings"
)

// MessageDecoder: 消息解码
// Author: yintongqiang
// Since:  2017/8/16
const (
	// 消息ID定长
	MSG_ID_LENGTH = 8 + 8

	// 存储记录各个字段位置
	MessageMagicCodePostion      = 4
	MessageFlagPostion           = 16
	MessagePhysicOffsetPostion   = 28
	MessageStoreTimestampPostion = 56
	MessageMagicCode             = 0xAABBCCDD ^ 1880681586 + 8
	charset                      = "utf-8"
	// 序列化消息属性
	NAME_VALUE_SEPARATOR = 1
	PROPERTY_SEPARATOR   = 2
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

func String2messageProperties(properties string) map[string]string {
	m := make(map[string]string)

	if len(properties) > 0 {
		items := strings.Split(properties, string(PROPERTY_SEPARATOR))
		if len(items) > 0 {
			for i := 0; i < len(items); i++ {
				nv := strings.Split(items[i], string(NAME_VALUE_SEPARATOR))
				if len(nv) == 2 {
					m[nv[1]] = nv[2]
				}
			}
		}
	}
	return m
}

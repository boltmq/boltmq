package protocol

import (
	"github.com/pquerna/ffjson/ffjson"
)

// RemotingSerializable 序列化
// Author gaoyanlei
// Since 2017/8/22
type RemotingSerializable struct {
}

//// Encode 只会序列化“RemotingSerializable”结构体的字段 ??
//func (self *RemotingSerializable) Encode() []byte {
//	if value, err := ffjson.Marshal(self); err == nil {
//		return value
//	}
//	return nil
//}
//
//// Decode 反序列化为self对应的结构体
//func (self *RemotingSerializable) Decode(data []byte) error {
//	return ffjson.Unmarshal(data, self)
//}

// CustomEncode 默认序列化参数v对应的结构体
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/15
func (self *RemotingSerializable) CustomEncode(v interface{}) []byte {
	if value, err := ffjson.Marshal(v); err == nil {
		return value
	}
	return nil
}

// CustomDecode 反序列化为传入参数v结构体
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/15
func (self *RemotingSerializable) CustomDecode(data []byte, v interface{}) error {
	return ffjson.Unmarshal(data, v)
}

package protocol

import "github.com/pquerna/ffjson/ffjson"

// RemotingSerializable 序列化
// Author gaoyanlei
// Since 2017/8/22
type RemotingSerializable struct {
}

func (self *RemotingSerializable) Encode() []byte {
	if value, err := ffjson.Marshal(self); err == nil {
		return value
	}
	return nil
}

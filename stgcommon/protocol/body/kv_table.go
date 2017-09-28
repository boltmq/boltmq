package body

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

type KVTable struct {
	Table map[string]string `json:"table"`
	*protocol.RemotingSerializable
}

func NewKVTable() *KVTable {
	var KVTable = new(KVTable)
	KVTable.Table = make(map[string]string)
	KVTable.RemotingSerializable = new(protocol.RemotingSerializable)
	return KVTable
}

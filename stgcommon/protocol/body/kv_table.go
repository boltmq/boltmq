package body

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

type KVTable struct {
	Table map[string]string
	*protocol.RemotingSerializable
}

func NewKVTable() *KVTable {
	var KVTable = new(KVTable)
	KVTable.Table = make(map[string]string)
	return KVTable
}

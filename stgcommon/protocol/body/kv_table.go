package body

type KVTable struct {
	Table map[string]string
}

func NewKVTable() *KVTable {
	var KVTable = new(KVTable)
	KVTable.Table = make(map[string]string)
	return KVTable
}

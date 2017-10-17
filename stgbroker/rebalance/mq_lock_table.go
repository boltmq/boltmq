package rebalance

import (
	"sync"
)

// MqLockTable MqLockTable
// Author rongzhihong
// Since 2017/9/20
type MqLockTable struct {
	mqLockTable map[string]*LockEntryTable // key: group
	sync.RWMutex
}

func NewMqLockTable() *MqLockTable {
	lockTable := new(MqLockTable)
	lockTable.mqLockTable = make(map[string]*LockEntryTable, 1024)
	return lockTable
}

func (lockTable *MqLockTable) Put(key string, value *LockEntryTable) {
	lockTable.Lock()
	defer lockTable.Unlock()
	lockTable.mqLockTable[key] = value
}

func (lockTable *MqLockTable) Get(key string) *LockEntryTable {
	lockTable.RLock()
	defer lockTable.RUnlock()

	v, ok := lockTable.mqLockTable[key]
	if !ok {
		return nil
	}
	return v
}

func (lockTable *MqLockTable) Remove(key string) {
	lockTable.Lock()
	defer lockTable.Unlock()

	_, ok := lockTable.mqLockTable[key]
	if !ok {
		return
	}
	delete(lockTable.mqLockTable, key)
}

func (lockTable *MqLockTable) Foreach(fn func(k string, v *LockEntryTable)) {
	lockTable.RLock()
	defer lockTable.RUnlock()

	for k, v := range lockTable.mqLockTable {
		fn(k, v)
	}
}

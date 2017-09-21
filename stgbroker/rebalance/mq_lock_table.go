package rebalance

import (
	"sync"
)

// MqLockTable MqLockTable
// Author rongzhihong
// Since 2017/9/20
type MqLockTable struct {
	mqLockTable     map[string]*LockEntryTable // key: group
	mqLockTableLock *sync.RWMutex
}

func NewMqLockTable() *MqLockTable {
	lockTable := new(MqLockTable)
	lockTable.mqLockTable = make(map[string]*LockEntryTable, 1024)
	return lockTable
}

func (lockTable *MqLockTable) Put(key string, value *LockEntryTable) {
	lockTable.mqLockTableLock.Lock()
	defer lockTable.mqLockTableLock.Unlock()
	lockTable.mqLockTable[key] = value
}

func (lockTable *MqLockTable) Get(key string) *LockEntryTable {
	lockTable.mqLockTableLock.Lock()
	defer lockTable.mqLockTableLock.Unlock()
	return lockTable.mqLockTable[key]
}

func (lockTable *MqLockTable) Remove(key string) {
	lockTable.mqLockTableLock.Lock()
	defer lockTable.mqLockTableLock.Unlock()
	delete(lockTable.mqLockTable, key)
}

func (lockTable *MqLockTable) Foreach(fn func(k string, v *LockEntryTable)) {
	lockTable.mqLockTableLock.Lock()
	defer lockTable.mqLockTableLock.Unlock()

	for k, v := range lockTable.mqLockTable {
		fn(k, v)
	}
}

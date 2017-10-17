package rebalance

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"sync"
)

// LockEntryTable LockEntryTable
// Author rongzhihong
// Since 2017/9/20
type LockEntryTable struct {
	lockEntryTable map[*message.MessageQueue]*body.LockEntry
	sync.RWMutex
}

func NewLockEntryTable() *LockEntryTable {
	lockTable := new(LockEntryTable)
	lockTable.lockEntryTable = make(map[*message.MessageQueue]*body.LockEntry, 32)
	return lockTable
}

func (lockTable *LockEntryTable) Put(key *message.MessageQueue, value *body.LockEntry) {
	lockTable.Lock()
	defer lockTable.Unlock()
	lockTable.lockEntryTable[key] = value
}

func (lockTable *LockEntryTable) Get(key *message.MessageQueue) *body.LockEntry {
	lockTable.Lock()
	defer lockTable.Unlock()

	v, ok := lockTable.lockEntryTable[key]
	if !ok {
		return nil
	}
	return v
}

func (lockTable *LockEntryTable) Remove(key *message.MessageQueue) {
	lockTable.Lock()
	defer lockTable.Unlock()

	_, ok := lockTable.lockEntryTable[key]
	if ok {
		return
	}
	delete(lockTable.lockEntryTable, key)
}

func (lockTable *LockEntryTable) Foreach(fn func(k *message.MessageQueue, v *body.LockEntry)) {
	lockTable.RLock()
	defer lockTable.RUnlock()

	for k, v := range lockTable.lockEntryTable {
		fn(k, v)
	}
}

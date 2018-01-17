// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package rebalance

import (
	"sync"

	"github.com/boltmq/common/message"
)

// LockEntryTable.lock.LockEntryTable
// Author rongzhihong
// Since 2017/9/20
type LockEntryTable struct {
	lockEntryTable map[*message.MessageQueue]*LockEntry
	lock           sync.RWMutex
}

func NewLockEntryTable() *LockEntryTable {
	lockTable := new(LockEntryTable)
	lockTable.lockEntryTable = make(map[*message.MessageQueue]*LockEntry, 32)
	return lockTable
}

func (lockTable *LockEntryTable) Put(key *message.MessageQueue, value *LockEntry) {
	lockTable.lock.Lock()
	defer lockTable.lock.Unlock()
	lockTable.lockEntryTable[key] = value
}

func (lockTable *LockEntryTable) Get(key *message.MessageQueue) *LockEntry {
	lockTable.lock.RLock()
	defer lockTable.lock.RUnlock()

	v, ok := lockTable.lockEntryTable[key]
	if !ok {
		return nil
	}
	return v
}

func (lockTable *LockEntryTable) Remove(key *message.MessageQueue) {
	lockTable.lock.Lock()
	defer lockTable.lock.Unlock()

	_, ok := lockTable.lockEntryTable[key]
	if ok {
		return
	}
	delete(lockTable.lockEntryTable, key)
}

func (lockTable *LockEntryTable) Foreach(fn func(k *message.MessageQueue, v *LockEntry)) {
	lockTable.lock.RLock()
	defer lockTable.lock.RUnlock()

	for k, v := range lockTable.lockEntryTable {
		fn(k, v)
	}
}

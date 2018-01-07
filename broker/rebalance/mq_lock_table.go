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
)

// MQLockTable MQLockTable
// Author rongzhihong
// Since 2017/9/20
type MQLockTable struct {
	mqLockTable map[string]*LockEntryTable // key: group
	lock        sync.RWMutex
}

func NewMQLockTable() *MQLockTable {
	lockTable := new(MQLockTable)
	lockTable.mqLockTable = make(map[string]*LockEntryTable, 1024)
	return lockTable
}

func (lockTable *MQLockTable) Put(key string, value *LockEntryTable) {
	lockTable.lock.Lock()
	defer lockTable.lock.Unlock()
	lockTable.mqLockTable[key] = value
}

func (lockTable *MQLockTable) Get(key string) *LockEntryTable {
	lockTable.lock.RLock()
	defer lockTable.lock.RUnlock()

	v, ok := lockTable.mqLockTable[key]
	if !ok {
		return nil
	}
	return v
}

func (lockTable *MQLockTable) Remove(key string) {
	lockTable.lock.Lock()
	defer lockTable.lock.Unlock()

	_, ok := lockTable.mqLockTable[key]
	if !ok {
		return
	}
	delete(lockTable.mqLockTable, key)
}

func (lockTable *MQLockTable) Foreach(fn func(k string, v *LockEntryTable)) {
	lockTable.lock.RLock()
	defer lockTable.lock.RUnlock()

	for k, v := range lockTable.mqLockTable {
		fn(k, v)
	}
}

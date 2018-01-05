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
package server

import (
	"sync"

	concurrent "github.com/fanliao/go-concurrentMap"
)

type OffsetTable struct {
	Offsets map[string]map[int]int64 `json:"offsets"`
	lock    sync.RWMutex             `json:"-"`
}

func newOffsetTable() *OffsetTable {
	return &OffsetTable{
		Offsets: make(map[string]map[int]int64),
	}
}

func (table *OffsetTable) Size() int {
	table.lock.RLock()
	defer table.lock.RUnlock()

	return len(table.Offsets)
}

func (table *OffsetTable) Put(k string, v map[int]int64) {
	table.lock.Lock()
	defer table.lock.Unlock()
	table.Offsets[k] = v
}

func (table *OffsetTable) Get(k string) map[int]int64 {
	table.lock.RLock()
	defer table.lock.RUnlock()

	v, ok := table.Offsets[k]
	if !ok {
		return nil
	}

	return v
}

func (table *OffsetTable) Remove(k string) map[int]int64 {
	table.lock.Lock()
	defer table.lock.Unlock()

	v, ok := table.Offsets[k]
	if !ok {
		return nil
	}

	delete(table.Offsets, k)
	return v
}

func (table *OffsetTable) Foreach(fn func(k string, v map[int]int64)) {
	table.lock.RLock()
	defer table.lock.RUnlock()

	for k, v := range table.Offsets {
		fn(k, v)
	}
}

func (table *OffsetTable) RemoveByFlag(fn func(k string, v map[int]int64) bool) {
	table.lock.Lock()
	for k, v := range table.Offsets {
		if fn(k, v) {
			delete(table.Offsets, k)
		}
	}
	table.lock.Unlock()
}

// PutAll 同步Offset配置文件
// Author rongzhihong
// Since 2017/9/18
func (table *OffsetTable) PutAll(offsetMap *concurrent.ConcurrentMap) {
	table.lock.Lock()
	defer table.lock.Unlock()

	for iter := offsetMap.Iterator(); iter.HasNext(); {
		kItem, vItem, _ := iter.Next()
		var k string = ""
		var ok bool = false
		if k, ok = kItem.(string); !ok {
			continue
		}
		if v, vok := vItem.(map[int]int64); vok {
			table.Offsets[k] = v
		}
	}
}

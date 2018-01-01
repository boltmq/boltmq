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
package stats

import (
	"sync"
)

// callSnapshot Call Snapshot
// Author rongzhihong
// Since 2017/9/19
type callSnapshot struct {
	timestamp int64 `json:"timestamp"`
	times     int64 `json:"times"`
	value     int64 `json:"value"`
}

func newCallSnapshot(timestamp, times int64) *callSnapshot {
	return &callSnapshot{
		timestamp: timestamp,
		times:     times,
	}
}

// callSnapshotPlus Call Snapshot安全数组
// Author rongzhihong
// Since 2017/9/19
type callSnapshotPlus struct {
	cslist []callSnapshot
	sync.RWMutex
}

func newCallSnapshotPlus() *callSnapshotPlus {
	return &callSnapshotPlus{
		cslist: make([]callSnapshot, 0, 25),
	}
}

func (csp *callSnapshotPlus) Size() int {
	csp.RLock()
	defer csp.RUnlock()
	return len(csp.cslist)
}

func (csp *callSnapshotPlus) Get(index int) callSnapshot {
	if index < 0 || index >= csp.Size() {
		return callSnapshot{}
	}

	csp.RLock()
	defer csp.RUnlock()
	return csp.cslist[index]
}

func (csp *callSnapshotPlus) Put(cs callSnapshot) {
	csp.Lock()
	defer csp.Unlock()
	csp.cslist = append(csp.cslist, cs)
}

func (csp *callSnapshotPlus) Remove(index int) bool {
	if index < 0 || index >= csp.Size() {
		return false
	}
	csp.Lock()
	defer csp.Unlock()

	csp.cslist = append(csp.cslist[:index], csp.cslist[index+1:]...)
	return true
}

// StatsSnapshot Stats Snapshot
// Author rongzhihong
// Since 2017/9/19
type StatsSnapshot struct {
	Sum   int64   `json:"sum"`
	Tps   float64 `json:"tps"`
	Avgpt float64 `json:"avgpt"`
}

// NewStatsSnapshot 初始化
// Author rongzhihong
// Since 2017/9/19
func NewStatsSnapshot() *StatsSnapshot {
	shot := new(StatsSnapshot)
	return shot
}

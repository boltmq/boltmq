package stats

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"sync"
)

// CallSnapshot Call Snapshot
// Author rongzhihong
// Since 2017/9/19
type CallSnapshot struct {
	Timestamp int64 `json:"timestamp"`
	Times     int64 `json:"times"`
	Value     int64 `json:"value"`
}

// CallSnapshotPlus Call Snapshot安全数组
// Author rongzhihong
// Since 2017/9/19
type CallSnapshotPlus struct {
	CsList []CallSnapshot
	sync.RWMutex
}

func NewCallSnapshotPlus() *CallSnapshotPlus {
	return &CallSnapshotPlus{
		CsList: make([]CallSnapshot, 0, 25),
	}
}

func (csp *CallSnapshotPlus) Size() int {
	csp.RLock()
	defer csp.RUnlock()
	return len(csp.CsList)
}

func (csp *CallSnapshotPlus) Get(index int) CallSnapshot {
	if index < 0 || index >= csp.Size() {
		return CallSnapshot{}
	}

	csp.RLock()
	defer csp.RUnlock()
	return csp.CsList[index]
}

func (csp *CallSnapshotPlus) Put(callSnapshot CallSnapshot) {
	csp.Lock()
	defer csp.Unlock()
	csp.CsList = append(csp.CsList, callSnapshot)
}

func (csp *CallSnapshotPlus) Remove(index int) bool {
	if index < 0 || index >= csp.Size() {
		return false
	}
	csp.Lock()
	defer csp.Unlock()
	defer utils.RecoveredFn()

	csp.CsList = append(csp.CsList[:index], csp.CsList[index+1:]...)
	return true
}

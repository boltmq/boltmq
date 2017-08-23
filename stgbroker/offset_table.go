package stgbroker

import "sync"

type OffsetTable struct {
	Offsets      map[string]map[int]int64 `json:"offsetTable"`
	sync.RWMutex `json:"-"`
}

func newOffsetTable() *OffsetTable {
	return &OffsetTable{
		Offsets: make(map[string]map[int]int64),
	}
}

func (table *OffsetTable) size() int {
	table.RLock()
	defer table.RUnlock()

	return len(table.Offsets)
}

func (table *OffsetTable) put(k string, v map[int]int64) {
	table.Lock()
	defer table.Unlock()
	table.Offsets[k] = v
}

func (table *OffsetTable) get(k string) map[int]int64 {
	table.RLock()
	defer table.RUnlock()

	v, ok := table.Offsets[k]
	if !ok {
		return nil
	}

	return v
}

func (table *OffsetTable) remove(k string) map[int]int64 {
	table.Lock()
	defer table.Unlock()

	v, ok := table.Offsets[k]
	if !ok {
		return nil
	}

	delete(table.Offsets, k)
	return v
}

func (table *OffsetTable) foreach(fn func(k string, v map[int]int64)) {
	table.RLock()
	defer table.RUnlock()

	for k, v := range table.Offsets {
		fn(k, v)
	}
}

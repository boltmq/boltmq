package client

import (
	"sync"
)

type ProducerGroupConnTable struct {
	GroupChannelTable map[string]map[string]*ChannelInfo // key:group value: map[channel.Addr()]ChannelInfo
	sync.RWMutex      `json:"-"`
}

func NewProducerGroupConnTable() *ProducerGroupConnTable {
	return &ProducerGroupConnTable{
		GroupChannelTable: make(map[string]map[string]*ChannelInfo),
	}
}

func (table *ProducerGroupConnTable) Size() int {
	table.RLock()
	defer table.RUnlock()

	return len(table.GroupChannelTable)
}

func (table *ProducerGroupConnTable) Put(k string, v map[string]*ChannelInfo) {
	table.Lock()
	defer table.Unlock()
	table.GroupChannelTable[k] = v
}

func (table *ProducerGroupConnTable) Get(k string) map[string]*ChannelInfo {
	table.RLock()
	defer table.RUnlock()

	v, ok := table.GroupChannelTable[k]
	if !ok {
		return nil
	}

	return v
}

func (table *ProducerGroupConnTable) Remove(k string) map[string]*ChannelInfo {
	table.Lock()
	defer table.Unlock()

	v, ok := table.GroupChannelTable[k]
	if !ok {
		return nil
	}

	delete(table.GroupChannelTable, k)
	return v
}

func (table *ProducerGroupConnTable) foreach(fn func(k string, v map[string]*ChannelInfo)) {
	table.RLock()
	defer table.RUnlock()

	for k, v := range table.GroupChannelTable {
		fn(k, v)
	}
}

// ForeachByWPerm 写操作迭代
// Author rongzhihong
// Since 2017/10/17
func (table *ProducerGroupConnTable) ForeachByWPerm(fn func(k string, v map[string]*ChannelInfo)) {
	table.Lock()
	defer table.Unlock()

	for k, v := range table.GroupChannelTable {
		fn(k, v)
	}
}

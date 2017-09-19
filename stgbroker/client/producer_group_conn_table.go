package client

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"sync"
)

type ProducerGroupConnTable struct {
	GroupChannelTable map[string]map[netm.Context]*ChannelInfo
	sync.RWMutex      `json:"-"`
}

func NewProducerGroupConnTable() *ProducerGroupConnTable {
	return &ProducerGroupConnTable{
		GroupChannelTable: make(map[string]map[netm.Context]*ChannelInfo),
	}
}

func (table *ProducerGroupConnTable) size() int {
	table.RLock()
	defer table.RUnlock()

	return len(table.GroupChannelTable)
}

func (table *ProducerGroupConnTable) put(k string, v map[netm.Context]*ChannelInfo) {
	table.Lock()
	defer table.Unlock()
	table.GroupChannelTable[k] = v
}

func (table *ProducerGroupConnTable) get(k string) map[netm.Context]*ChannelInfo {
	table.RLock()
	defer table.RUnlock()

	v, ok := table.GroupChannelTable[k]
	if !ok {
		return nil
	}

	return v
}

func (table *ProducerGroupConnTable) remove(k string) map[netm.Context]*ChannelInfo {
	table.Lock()
	defer table.Unlock()

	v, ok := table.GroupChannelTable[k]
	if !ok {
		return nil
	}

	delete(table.GroupChannelTable, k)
	return v
}

func (table *ProducerGroupConnTable) foreach(fn func(k string, v map[netm.Context]*ChannelInfo)) {
	table.RLock()
	defer table.RUnlock()

	for k, v := range table.GroupChannelTable {
		fn(k, v)
	}
}

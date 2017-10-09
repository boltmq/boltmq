package client

import (
	"sync"
)

type ConsumerTable struct {
	GroupChannelTable map[string]*ConsumerGroupInfo
	sync.RWMutex      `json:"-"`
}

func NewConsumerTable() *ConsumerTable {
	return &ConsumerTable{
		GroupChannelTable: make(map[string]*ConsumerGroupInfo),
	}
}

func (table *ConsumerTable) size() int {
	table.RLock()
	defer table.RUnlock()

	return len(table.GroupChannelTable)
}

func (table *ConsumerTable) put(k string, v *ConsumerGroupInfo) {
	table.Lock()
	defer table.Unlock()
	table.GroupChannelTable[k] = v
}

func (table *ConsumerTable) get(k string) *ConsumerGroupInfo {
	table.RLock()
	defer table.RUnlock()

	v, ok := table.GroupChannelTable[k]
	if !ok {
		return nil
	}

	return v
}

func (table *ConsumerTable) remove(k string) *ConsumerGroupInfo {
	table.Lock()
	defer table.Unlock()

	v, ok := table.GroupChannelTable[k]
	if !ok {
		return nil
	}

	delete(table.GroupChannelTable, k)
	return v
}

func (table *ConsumerTable) foreach(fn func(k string, v *ConsumerGroupInfo)) {
	table.RLock()
	defer table.RUnlock()

	for k, v := range table.GroupChannelTable {
		fn(k, v)
	}
}

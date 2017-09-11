package subscription

import (
	"sync"
)

type SubscriptionGroupTable struct {
	SubscriptionGroupTable map[string]*SubscriptionGroupConfig `json:"subscriptionGroupTable"`
	sync.RWMutex `json:"-"`
}

func NewSubscriptionGroupTable() *SubscriptionGroupTable {
	return &SubscriptionGroupTable{
		SubscriptionGroupTable: make(map[string]*SubscriptionGroupConfig),
	}
}

func (table *SubscriptionGroupTable) Size() int {
	table.RLock()
	defer table.RUnlock()

	return len(table.SubscriptionGroupTable)
}

func (table *SubscriptionGroupTable) Put(k string, v *SubscriptionGroupConfig) *SubscriptionGroupConfig {
	table.Lock()
	defer table.Unlock()
	table.SubscriptionGroupTable[k] = v
	return v
}

func (table *SubscriptionGroupTable) Get(k string) *SubscriptionGroupConfig {
	table.RLock()
	defer table.RUnlock()

	v, ok := table.SubscriptionGroupTable[k]
	if !ok {
		return nil
	}

	return v
}

func (table *SubscriptionGroupTable) Remove(k string) *SubscriptionGroupConfig {
	table.Lock()
	defer table.Unlock()

	v, ok := table.SubscriptionGroupTable[k]
	if !ok {
		return nil
	}

	delete(table.SubscriptionGroupTable, k)
	return v
}

func (table *SubscriptionGroupTable) Foreach(fn func(k string, v *SubscriptionGroupConfig)) {
	table.RLock()
	defer table.RUnlock()

	for k, v := range table.SubscriptionGroupTable {
		fn(k, v)
	}
}

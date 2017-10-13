package subscription

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	syncmap "git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"sync"
)

type SubscriptionGroupTable struct {
	SubscriptionGroupTable map[string]*SubscriptionGroupConfig `json:"subscriptionGroupTable"`
	DataVersion            stgcommon.DataVersion               `json:"dataVersion"`
	sync.RWMutex           `json:"-"`
}

func NewSubscriptionGroupTable() *SubscriptionGroupTable {
	subscriptionGroupTable := &SubscriptionGroupTable{
		SubscriptionGroupTable: make(map[string]*SubscriptionGroupConfig, 1024),
		DataVersion:            *stgcommon.NewDataVersion(),
	}
	return subscriptionGroupTable
}

func (table *SubscriptionGroupTable) Size() int {
	table.RLock()
	defer table.RUnlock()

	return len(table.SubscriptionGroupTable)
}

func (table *SubscriptionGroupTable) Put(k string, v *SubscriptionGroupConfig) *SubscriptionGroupConfig {
	table.Lock()
	defer table.Unlock()
	old := table.SubscriptionGroupTable[k]
	table.SubscriptionGroupTable[k] = v
	return old
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

// Clear 清空
// Author rongzhihong
// Since 2017/9/18
func (table *SubscriptionGroupTable) Clear() {
	table.RLock()
	defer table.RUnlock()

	table.SubscriptionGroupTable = make(map[string]*SubscriptionGroupConfig, 1024)
}

// syncTopicConfig 同步Topic配置文件
// Author rongzhihong
// Since 2017/9/18
func (table *SubscriptionGroupTable) PutAll(offsetMap *syncmap.Map) {
	table.Lock()
	defer table.Unlock()

	if offsetMap == nil {
		return
	}

	itor := offsetMap.Iterator()
	for itor.HasNext() {
		key, value, _ := itor.Next()
		if groupName, ok := key.(string); ok && key != "" {
			if subscriptionGroupConfig, ok := value.(*SubscriptionGroupConfig); ok {
				table.SubscriptionGroupTable[groupName] = subscriptionGroupConfig
			}
		}
	}
}

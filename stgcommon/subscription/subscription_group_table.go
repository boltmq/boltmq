package subscription

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	syncmap "git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"sync"
)

type SubscriptionGroupTable struct {
	SubscriptionGroupTable map[string]*SubscriptionGroupConfig `json:"subscriptionGroupTable"` // key:group
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
	table.Lock()
	defer table.Unlock()

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

	for iter := offsetMap.Iterator(); iter.HasNext(); {
		key, value, _ := iter.Next()
		if groupName, ok := key.(string); ok && groupName != "" {
			if subscriptionGroupConfig, ok := value.(*SubscriptionGroupConfig); ok {
				table.SubscriptionGroupTable[groupName] = subscriptionGroupConfig
			}
		}
	}
}

// ClearAndPutAll 清空map,再PutAll
// Author rongzhihong
// Since 2017/9/18
func (table *SubscriptionGroupTable) ClearAndPutAll(offsetMap *syncmap.Map) {
	table.Lock()
	defer table.Unlock()

	table.SubscriptionGroupTable = make(map[string]*SubscriptionGroupConfig, 1024)
	if offsetMap == nil {
		return
	}

	for iter := offsetMap.Iterator(); iter.HasNext(); {
		key, value, _ := iter.Next()
		if groupName, ok := key.(string); ok && groupName != "" {
			if subscriptionGroupConfig, ok := value.(*SubscriptionGroupConfig); ok {
				table.SubscriptionGroupTable[groupName] = subscriptionGroupConfig
			}
		}
	}
}

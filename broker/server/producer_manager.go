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
	"container/list"
	"math/rand"
	"sync"
	"time"

	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/net/core"
	"github.com/boltmq/common/utils/codec"
	"github.com/boltmq/common/utils/system"
)

type producerManager struct {
	lockTimeoutMillis     int64
	channelExpiredTimeout int64
	groupChannelTable     *producerGroupConnTable
	groupChannelLock      sync.RWMutex
	hashcodeChannelTable  map[int64]*list.List
	hashCodeChannelLock   sync.RWMutex
	random                *rand.Rand
}

func newProducerManager() *producerManager {
	var pm = new(producerManager)
	pm.lockTimeoutMillis = 3000
	pm.channelExpiredTimeout = 1000 * 120
	pm.groupChannelTable = newProducerGroupConnTable()
	pm.hashcodeChannelTable = make(map[int64]*list.List)
	pm.random = rand.New(rand.NewSource(time.Now().UnixNano()))
	return pm
}

func (pm *producerManager) generateRandmonNum() int {
	return pm.random.Int()
}

// getgroupChannelTable 获得组通道
// Author gaoyanlei
// Since 2017/8/24
func (pm *producerManager) getGroupChannelTable() *producerGroupConnTable {
	ngct := newProducerGroupConnTable()
	pm.groupChannelLock.Lock()
	defer pm.groupChannelLock.Unlock()

	pm.groupChannelTable.foreach(func(k string, v map[string]*channelInfo) {
		ngct.tables[k] = v
	})
	return ngct
}

// registerProducer producer注册
// Author gaoyanlei
// Since 2017/8/24
func (pm *producerManager) registerProducer(group string, chanInfo *channelInfo) {
	pm.groupChannelLock.Lock()

	channelTable := pm.groupChannelTable.Get(group)
	if nil == channelTable {
		channelTable = make(map[string]*channelInfo)
		pm.groupChannelTable.Put(group, channelTable)
	}

	clientChannelInfoFound, ok := channelTable[chanInfo.ctx.UniqueSocketAddr().String()]
	if !ok || nil == clientChannelInfoFound {
		channelTable[chanInfo.ctx.UniqueSocketAddr().String()] = chanInfo
		logger.Infof("new producer connected, group: %s channel: %s.", group, chanInfo.addr)
	}

	if clientChannelInfoFound != nil {
		clientChannelInfoFound.lastUpdateTimestamp = system.CurrentTimeMillis()
	}

	pm.groupChannelLock.Unlock()

	// 事务消息
	bClientChannelInfoFound := false

	pm.hashCodeChannelLock.Lock()
	groupdHashCode := codec.HashCode(group)

	channelList, ok := pm.hashcodeChannelTable[groupdHashCode]
	if !ok || nil == channelList {
		channelList = list.New()
		pm.hashcodeChannelTable[groupdHashCode] = channelList
	}

	bClientChannelInfoFound, _ = contains(channelList, chanInfo)
	if !bClientChannelInfoFound {
		channelList.PushBack(chanInfo)
		logger.Infof("new producer connected, group: %s, group.hashcode: %d, %s.",
			group, groupdHashCode, chanInfo)
	}

	if bClientChannelInfoFound && clientChannelInfoFound != nil {
		clientChannelInfoFound.lastUpdateTimestamp = system.CurrentTimeMillis()
	}

	pm.hashCodeChannelLock.Unlock()
}

// unregisterProducer 注销producer
// Author gaoyanlei
// Since 2017/8/24
func (pm *producerManager) unregisterProducer(group string, chanInfo *channelInfo) {
	pm.groupChannelLock.Lock()

	connTable := pm.groupChannelTable.Get(group)
	if nil != connTable {
		delete(connTable, chanInfo.ctx.UniqueSocketAddr().String())
		logger.Infof("unregister a producer %s from groupChannelTable %s.", group, chanInfo.addr)

		if pm.groupChannelTable.Size() <= 0 {
			pm.groupChannelTable.Remove(group)
			logger.Infof("unregister a producer %s from group channel table.", group)
		}
	}

	pm.groupChannelLock.Unlock()

	// 事务消息
	pm.hashCodeChannelLock.Lock()

	groupHashCode := codec.HashCode(group)
	channelList, ok := pm.hashcodeChannelTable[groupHashCode]
	if ok && nil != channelList && channelList.Len() > 0 {
		isRemove := remove(channelList, chanInfo)
		if isRemove {
			logger.Infof("unregister a producer[%s] from hashcode channel table %s.", group, chanInfo.addr)
		}

		if channelList.Len() <= 0 {
			delete(pm.hashcodeChannelTable, groupHashCode)
			logger.Infof("unregister a producer group[%s] from hashcode channel table.", group)
		}
	}

	pm.hashCodeChannelLock.Unlock()
}

// scanNotActiveChannel 扫描不活跃通道
// Author rongzhihong
// Since 2017/9/17
func (pm *producerManager) scanNotActiveChannel() {
	pm.groupChannelLock.Lock()
	defer pm.groupChannelLock.Unlock()

	pm.groupChannelTable.ForeachByWPerm(func(group string, chlMap map[string]*channelInfo) {
		for key, info := range chlMap {
			diff := system.CurrentTimeMillis() - info.lastUpdateTimestamp
			if diff > pm.channelExpiredTimeout {
				delete(chlMap, key)
				logger.Warnf("SCAN: remove expired channel[%s] from producerManager groupChannelTable, producer group name: %s.",
					info.ctx.RemoteAddr(), group)
				info.ctx.Close()
			}
		}
		if len(chlMap) <= 0 {
			delete(pm.groupChannelTable.tables, group)
		}
	})
}

// doChannelCloseEvent 通道关闭事件
// Author rongzhihong
// Since 2017/9/17
func (pm *producerManager) doChannelCloseEvent(remoteAddr string, ctx core.Context) {
	pm.groupChannelLock.Lock()
	defer pm.groupChannelLock.Unlock()

	pm.groupChannelTable.ForeachByWPerm(func(group string, clientChannelInfoTable map[string]*channelInfo) {
		_, ok := clientChannelInfoTable[ctx.UniqueSocketAddr().String()]
		if ok {
			delete(clientChannelInfoTable, ctx.UniqueSocketAddr().String())
			logger.Infof("NETTY EVENT: remove channel[%s] from producerManager groupChannelTable, producer group: %s.",
				remoteAddr, group)
		}
		if len(clientChannelInfoTable) <= 0 {
			delete(pm.groupChannelTable.tables, group)
		}
	})
}

// pickProducerChannelRandomly 事务消息
// Author rongzhihong
// Since 2017/9/17
func (pm *producerManager) pickProducerChannelRandomly(producerGroupHashCode int) *channelInfo {
	pm.hashCodeChannelLock.Lock()
	defer pm.hashCodeChannelLock.Unlock()

	channelInfoList, ok := pm.hashcodeChannelTable[int64(producerGroupHashCode)]
	if ok && channelInfoList != nil && channelInfoList.Len() > 0 {
		index := pm.generateRandmonNum() % channelInfoList.Len()
		if index >= 0 && index < channelInfoList.Len() {
			info := get(channelInfoList, index)
			if channel, ok := info.(*channelInfo); ok {
				return channel
			}
		}
	}
	return nil
}

// contains 判断列表是否包含某元素
// Author rongzhihong
// Since 2017/9/17
func contains(lst *list.List, value *channelInfo) (bool, *list.Element) {
	for e := lst.Front(); e != nil; e = e.Next() {
		if info, ok := (e.Value).(*channelInfo); ok && info.addr == value.addr {
			return true, e
		}
	}
	return false, nil
}

// remove 删除列表中某元素
// Author rongzhihong
// Since 2017/9/17
func remove(lst *list.List, value *channelInfo) bool {
	if isContain, e := contains(lst, value); isContain {
		lst.Remove(e)
		return true
	}
	return false
}

// remove 获得列表中某元素
// Author rongzhihong
// Since 2017/9/17
func get(lst *list.List, index int) interface{} {
	pos := 0
	for e := lst.Front(); e != nil; e = e.Next() {
		if index == pos {
			return e.Value
		}
		pos++
	}
	return nil
}

type producerGroupConnTable struct {
	tables map[string]map[string]*channelInfo `json:"tables"` // key:group value: map[channel.Addr()]channelInfo
	lock   sync.RWMutex                       `json:"-"`
}

func newProducerGroupConnTable() *producerGroupConnTable {
	return &producerGroupConnTable{
		tables: make(map[string]map[string]*channelInfo),
	}
}

func (table *producerGroupConnTable) Size() int {
	table.lock.RLock()
	defer table.lock.RUnlock()

	return len(table.tables)
}

func (table *producerGroupConnTable) Put(k string, v map[string]*channelInfo) {
	table.lock.Lock()
	defer table.lock.Unlock()
	table.tables[k] = v
}

func (table *producerGroupConnTable) Get(k string) map[string]*channelInfo {
	table.lock.RLock()
	defer table.lock.RUnlock()

	v, ok := table.tables[k]
	if !ok {
		return nil
	}

	return v
}

func (table *producerGroupConnTable) Remove(k string) map[string]*channelInfo {
	table.lock.Lock()
	defer table.lock.Unlock()

	v, ok := table.tables[k]
	if !ok {
		return nil
	}

	delete(table.tables, k)
	return v
}

func (table *producerGroupConnTable) foreach(fn func(k string, v map[string]*channelInfo)) {
	table.lock.RLock()
	defer table.lock.RUnlock()

	for k, v := range table.tables {
		fn(k, v)
	}
}

// ForeachByWPerm 写操作迭代
// Author rongzhihong
// Since 2017/10/17
func (table *producerGroupConnTable) ForeachByWPerm(fn func(k string, v map[string]*channelInfo)) {
	table.lock.Lock()
	defer table.lock.Unlock()

	for k, v := range table.tables {
		fn(k, v)
	}
}

package client

import (
	"container/list"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"math/rand"
	"sync"
	"time"
)

type ProducerManager struct {
	LockTimeoutMillis     int64
	ChannelExpiredTimeout int64
	GroupChannelTable     *ProducerGroupConnTable
	GroupChannelLock      *sync.RWMutex
	hashcodeChannelTable  map[int64]*list.List
	HashCodeChannelLock   *sync.RWMutex
	Rand                  *rand.Rand
}

func NewProducerManager() *ProducerManager {
	var brokerController = new(ProducerManager)
	brokerController.LockTimeoutMillis = 3000
	brokerController.ChannelExpiredTimeout = 1000 * 120
	brokerController.GroupChannelTable = NewProducerGroupConnTable()
	brokerController.hashcodeChannelTable = make(map[int64]*list.List)
	brokerController.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	return brokerController
}

func (pm *ProducerManager) generateRandmonNum() int {
	return pm.Rand.Int()
}

// GetGroupChannelTable 获得组通道
// Author gaoyanlei
// Since 2017/8/24
func (pm *ProducerManager) GetGroupChannelTable() {
	newGroupChannelTable := make(map[string]map[netm.Context]*ChannelInfo)
	pm.GroupChannelLock.Lock()
	defer pm.GroupChannelLock.Unlock()

	pm.GroupChannelTable.foreach(func(k string, v map[netm.Context]*ChannelInfo) {
		newGroupChannelTable[k] = v
	})
}

// registerProducer producer注册
// Author gaoyanlei
// Since 2017/8/24
func (pm *ProducerManager) RegisterProducer(group string, channelInfo *ChannelInfo) {
	pm.GroupChannelLock.Lock()

	connTable := pm.GroupChannelTable.get(group)
	if nil == connTable {
		channelTable := make(map[netm.Context]*ChannelInfo)
		pm.GroupChannelTable.put(group, channelTable)
	}

	clientChannelInfoFound, ok := connTable[channelInfo.Context]
	if !ok || nil == clientChannelInfoFound {
		connTable[channelInfo.Context] = channelInfo
		logger.Infof("new producer connected, group: %s channel: %s", group, channelInfo.Addr)
	}

	if clientChannelInfoFound != nil {
		clientChannelInfoFound.LastUpdateTimestamp = timeutil.CurrentTimeMillis()
	}

	pm.GroupChannelLock.Unlock()

	// 事务消息
	bClientChannelInfoFound := false

	pm.HashCodeChannelLock.Lock()
	groupdHashCode := stgcommon.HashCode(group)

	channelList, ok := pm.hashcodeChannelTable[groupdHashCode]
	if !ok || nil == channelList {
		channelList = list.New()
		pm.hashcodeChannelTable[groupdHashCode] = channelList
	}

	bClientChannelInfoFound, _ = contains(channelList, channelInfo)
	if !bClientChannelInfoFound {
		channelList.PushBack(channelInfo)
		logger.Infof("new producer connected, group: %s group hashcode: %s channel: %v", group,
			groupdHashCode, channelInfo)
	}

	if bClientChannelInfoFound {
		clientChannelInfoFound.LastUpdateTimestamp = timeutil.CurrentTimeMillis()
	}

	pm.HashCodeChannelLock.Unlock()
}

// UnregisterProducer 注销producer
// Author gaoyanlei
// Since 2017/8/24
func (pm *ProducerManager) UnregisterProducer(group string, channelInfo *ChannelInfo) {
	pm.GroupChannelLock.Lock()

	connTable := pm.GroupChannelTable.get(group)
	if nil != connTable {
		delete(connTable, channelInfo.Context)
		logger.Infof("unregister a producer %s from groupChannelTable %s", group, channelInfo.Addr)

		if pm.GroupChannelTable.size() <= 0 {
			pm.GroupChannelTable.remove(group)
			logger.Infof("unregister a producer %s from groupChannelTable", group)
		}
	}

	pm.GroupChannelLock.Unlock()

	// 事务消息
	pm.HashCodeChannelLock.Lock()

	groupHashCode := stgcommon.HashCode(group)
	channelList, ok := pm.hashcodeChannelTable[groupHashCode]
	if ok && nil != channelList && channelList.Len() > 0 {
		isRemove := remove(channelList, channelInfo)
		if isRemove {
			logger.Infof("unregister a producer[%s] from hashcodeChannelTable %s", group, channelInfo.Addr)
		}

		if channelList.Len() <= 0 {
			delete(pm.hashcodeChannelTable, groupHashCode)
			logger.Infof("unregister a producer group[%s] from hashcodeChannelTable", group)
		}
	}

	pm.HashCodeChannelLock.Unlock()
}

// ScanNotActiveChannel 扫描不活跃通道
// Author rongzhihong
// Since 2017/9/17
func (pm *ProducerManager) ScanNotActiveChannel() {
	pm.GroupChannelLock.Lock()
	defer pm.GroupChannelLock.Unlock()

	for group, chlMap := range pm.GroupChannelTable.GroupChannelTable {
		for key, info := range chlMap {
			diff := timeutil.CurrentTimeMillis() - info.LastUpdateTimestamp
			if diff > pm.ChannelExpiredTimeout {
				delete(chlMap, key)
				logger.Warnf("SCAN: remove expired channel[%s] from ProducerManager groupChannelTable, producer group name: %s",
					info.Context.RemoteAddr(), group)
				info.Context.Close()
			}
		}
	}
}

// DoChannelCloseEvent 通道关闭事件
// Author rongzhihong
// Since 2017/9/17
func (pm *ProducerManager) DoChannelCloseEvent(remoteAddr string, ctx netm.Context) {
	pm.GroupChannelLock.Lock()
	defer pm.GroupChannelLock.Unlock()

	for group, clientChannelInfoTable := range pm.GroupChannelTable.GroupChannelTable {
		delete(clientChannelInfoTable, ctx)
		logger.Infof("NETTY EVENT: remove channel[%s] from ProducerManager groupChannelTable, producer group: %s",
			remoteAddr, group)
	}
}

// PickProducerChannelRandomly 事务消息
// Author rongzhihong
// Since 2017/9/17
func (pm *ProducerManager) PickProducerChannelRandomly(producerGroupHashCode int) *ChannelInfo {
	channelInfoList, ok := pm.hashcodeChannelTable[int64(producerGroupHashCode)]
	if ok && channelInfoList != nil && channelInfoList.Len() > 0 {
		index := pm.generateRandmonNum() % channelInfoList.Len()
		if index >= 0 && index < channelInfoList.Len() {
			info := get(channelInfoList, index)
			if channel, ok := info.(*ChannelInfo); ok {
				return channel
			}
		}
	}
	return nil
}

// contains 判断列表是否包含某元素
// Author rongzhihong
// Since 2017/9/17
func contains(lst *list.List, value *ChannelInfo) (bool, *list.Element) {
	for e := lst.Front(); e != nil; e = e.Next() {
		if e.Value == value {
			return true, e
		}
	}
	return false, nil
}

// remove 删除列表中某元素
// Author rongzhihong
// Since 2017/9/17
func remove(lst *list.List, value *ChannelInfo) bool {
	if isContain, e := contains(lst, value); isContain {
		lst.Remove(e)
		return true
	}
	return false
}

// remove 删除列表中某元素
// Author rongzhihong
// Since 2017/9/17
func get(lst *list.List, index int) interface{} {
	pos := 0
	for e := lst.Front(); e != nil; e = e.Next() {
		if index == pos {
			return e.Value
		}
	}
	return nil
}

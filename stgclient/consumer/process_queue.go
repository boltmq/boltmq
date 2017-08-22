package consumer

import (
	"time"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"sort"
	"sync/atomic"
	"strings"
	"strconv"
	"sync"
)
// ProcessQueue: 消息处理队列
// Author: yintongqiang
// Since:  2017/8/10

type ProcessQueue struct {
	sync.RWMutex
	Dropped           bool
	LastPullTimestamp int64
	PullMaxIdleTime   int64
	MsgCount          int64
	MsgTreeMap        *TreeMap
	QueueOffsetMax    int64
	Consuming         bool
	MsgAccCnt         int64
}
type TreeMap struct {
	keys     []int
	innerMap map[int]*message.MessageExt
}

func NewTreeMap() *TreeMap {
	return &TreeMap{
		innerMap:make(map[int]*message.MessageExt)}
}

func (treeMap *TreeMap)put(offset int, msg *message.MessageExt) *message.MessageExt {
	treeMap.innerMap[offset] = msg
	treeMap.keys = append(treeMap.keys, offset)
	sort.Ints(treeMap.keys)
	return msg
}

func (treeMap *TreeMap)get(offset int) *message.MessageExt {
	return treeMap.innerMap[offset]
}

func (treeMap *TreeMap)firstKey() int {
	return treeMap.keys[0]
}

func (treeMap *TreeMap)remove(offset int) *message.MessageExt {
	msg := treeMap.innerMap[offset]
	newKeys := []int{}
	for _, key := range treeMap.keys {
		if key != offset {
			newKeys = append(newKeys, key)
		}
	}
	sort.Ints(newKeys)
	treeMap.keys = newKeys
	delete(treeMap.innerMap, offset)
	return msg
}

func NewProcessQueue() *ProcessQueue {
	return &ProcessQueue{
		PullMaxIdleTime:120000,
		MsgTreeMap:NewTreeMap(),
	}
}

func (pq *ProcessQueue) IsPullExpired() bool {
	return (time.Now().Unix() * 1000 - pq.LastPullTimestamp) > pq.PullMaxIdleTime
}

func (pq *ProcessQueue) PutMessage(msgs []message.MessageExt) bool {
	pq.Lock()
	defer pq.Unlock()
	dispatchToConsume := false
	var validMsgCnt int32 = 0
	for _, msg := range msgs {
		old := pq.MsgTreeMap.put(msg.QueueId, &msg)
		if old == nil {
			validMsgCnt++
			pq.QueueOffsetMax = msg.QueueOffset
		}

	}
	atomic.AddInt32(&validMsgCnt, 1)
	if len(pq.MsgTreeMap.innerMap) > 0&& !pq.Consuming {
		dispatchToConsume = true
		pq.Consuming = true
	}
	if len(msgs) > 0 {
		messageExt := msgs[len(msgs) - 1]
		property := messageExt.Properties[message.PROPERTY_MAX_OFFSET]
		if !strings.EqualFold(property, "") {
			maxOffset, _ := strconv.ParseInt(property, 10, 64)
			accTotal := maxOffset - messageExt.QueueOffset
			if accTotal > 0 {
				pq.MsgAccCnt = accTotal
			}
		}
	}
	return dispatchToConsume
}

func (pq *ProcessQueue) RemoveMessage(msgs []message.MessageExt) int64 {
	var result int64= -1
	pq.Lock()
	defer pq.Unlock()
	if len(pq.MsgTreeMap.innerMap) > 0 {
		result = pq.QueueOffsetMax + 1
		var removedCnt int64 = 0
		for _, msg := range msgs {
			prev := pq.MsgTreeMap.remove(int(msg.QueueOffset))
			if prev != nil {
				removedCnt--
			}
		}
		atomic.AddInt64(&pq.MsgCount, removedCnt)
		if len(pq.MsgTreeMap.innerMap) > 0 {
			result = int64(pq.MsgTreeMap.firstKey())
		}
	}

	return result
}
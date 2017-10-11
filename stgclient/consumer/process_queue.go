package consumer

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ProcessQueue: 消息处理队列
// Author: yintongqiang
// Since:  2017/8/10

type ProcessQueue struct {
	lockConsume       sync.RWMutex
	lockTreeMap       sync.RWMutex
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
	sync.RWMutex
	keys     []int
	innerMap map[int]*message.MessageExt
}

func NewTreeMap() *TreeMap {
	return &TreeMap{
		innerMap: make(map[int]*message.MessageExt)}
}

func (treeMap *TreeMap) put(offset int, msg *message.MessageExt) *message.MessageExt {
	old:=treeMap.innerMap[offset]
	treeMap.innerMap[offset] = msg
	treeMap.keys = append(treeMap.keys, offset)
	sort.Ints(treeMap.keys)
	return old
}

func (treeMap *TreeMap) get(offset int) *message.MessageExt {
	return treeMap.innerMap[offset]
}

func (treeMap *TreeMap) firstKey() int {
	return treeMap.keys[0]
}

func (treeMap *TreeMap) lastKey() int {
	return treeMap.keys[len(treeMap.keys)-1]
}

func (treeMap *TreeMap) remove(offset int) *message.MessageExt {
	treeMap.Lock()
	defer treeMap.Unlock()
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
		PullMaxIdleTime:   120000,
		LastPullTimestamp: time.Now().Unix() * 1000,
		MsgTreeMap:        NewTreeMap(),
	}
}

func (pq *ProcessQueue) IsPullExpired() bool {
	return (time.Now().Unix()*1000 - pq.LastPullTimestamp) > pq.PullMaxIdleTime
}

func (pq *ProcessQueue) PutMessage(msgs []*message.MessageExt) bool {
	pq.lockTreeMap.Lock()
	defer pq.lockTreeMap.Unlock()
	dispatchToConsume := false
	var validMsgCnt int64 = 0
	for _, msg := range msgs {
		old := pq.MsgTreeMap.put(int(msg.QueueOffset), msg)
		if old == nil {
			validMsgCnt++
			pq.QueueOffsetMax = msg.QueueOffset
		}

	}
	atomic.AddInt64(&pq.MsgCount, validMsgCnt)
	if len(pq.MsgTreeMap.innerMap) > 0 && !pq.Consuming {
		dispatchToConsume = true
		pq.Consuming = true
	}
	if len(msgs) > 0 {
		messageExt := msgs[len(msgs)-1]
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

func (pq *ProcessQueue) RemoveMessage(msgs []*message.MessageExt) int64 {
	pq.lockTreeMap.Lock()
	defer pq.lockTreeMap.Unlock()
	var result int64 = -1
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

func (pq *ProcessQueue) GetMaxSpan() int64 {
	defer pq.lockTreeMap.Unlock()
	pq.lockTreeMap.Lock()
	if len(pq.MsgTreeMap.innerMap) > 0 {
		return int64(pq.MsgTreeMap.lastKey() - pq.MsgTreeMap.firstKey())
	}
	return 0
}

func (pq *ProcessQueue) ToString() string {
	return fmt.Sprintf("ProcessQueue[LastPullTimestamp=%v,MsgCount=%v,MsgAccCnt=%v]", pq.LastPullTimestamp, pq.MsgCount, pq.MsgAccCnt)
}

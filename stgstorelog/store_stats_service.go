package stgstorelog

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

const (
	FrequencyOfSampling  = 1000
	MaxRecordsOfSampling = 60 * 10
	PrintTPSInterval     = 60 * 1
)

type StoreStatsService struct {
	putMessageFailedTimes        int64
	putMessageTopicTimesTotal    map[string]int64
	putMessageTopicSizeTotal     map[string]int64
	getMessageTimesTotalFound    int64
	getMessageTransferedMsgCount int64
	getMessageTimesTotalMiss     int64
	putMessageDistributeTime     []int64
	putTimesList                 *list.List
	getTimesFoundList            *list.List
	getTimesMissList             *list.List
	transferedMsgCountList       *list.List
	messageStoreBootTimestamp    int64
	putMessageEntireTimeMax      int64
	getMessageEntireTimeMax      int64
	lockPut                      *sync.Mutex
	lockGet                      *sync.Mutex
	dispatchMaxBuffer            int64
	lockSampling                 *sync.Mutex
	lastPrintTimestamp           int64
}

func NewStoreStatsService() *StoreStatsService {
	service := new(StoreStatsService)
	service.putMessageDistributeTime = make([]int64, 7)
	service.putTimesList = list.New()
	service.getTimesFoundList = list.New()
	service.getTimesMissList = list.New()
	service.transferedMsgCountList = list.New()
	service.lockPut = new(sync.Mutex)
	service.lockGet = new(sync.Mutex)
	service.lockSampling = new(sync.Mutex)
	atomic.StoreInt64(&service.messageStoreBootTimestamp, time.Now().UnixNano()/1000000)
	atomic.StoreInt64(&service.lastPrintTimestamp, time.Now().UnixNano()/1000000)

	return service
}

func (self *StoreStatsService) Start() {
	// TODO
}

func (self *StoreStatsService) setGetMessageEntireTimeMax(value int64) {
	if value > self.getMessageEntireTimeMax {
		self.lockGet.Lock()
		self.getMessageEntireTimeMax = value
		self.lockGet.Unlock()
	}
}

func (self *StoreStatsService) GetGetMessageTransferedMsgCount() int64 {
	return atomic.LoadInt64(&self.getMessageTransferedMsgCount)
}

func (self *StoreStatsService) GetPutMessageTimesTotal() int64 {
	result := int64(0)
	for _, data := range self.putMessageTopicSizeTotal {
		atomic.AddInt64(&result, atomic.LoadInt64(&data))
	}

	return result
}

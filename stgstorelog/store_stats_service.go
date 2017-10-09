package stgstorelog

import (
	"bytes"
	"container/list"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
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
	service.putMessageTopicTimesTotal = make(map[string]int64, 128)
	service.putMessageTopicSizeTotal = make(map[string]int64, 128)
	service.getMessageTimesTotalFound = 0
	service.getMessageTransferedMsgCount = 0
	service.getMessageTimesTotalMiss = 0
	service.putMessageDistributeTime = make([]int64, 7)
	service.putTimesList = list.New()
	service.getTimesFoundList = list.New()
	service.getTimesMissList = list.New()
	service.transferedMsgCountList = list.New()
	service.lockPut = new(sync.Mutex)
	service.lockGet = new(sync.Mutex)
	service.lockSampling = new(sync.Mutex)
	service.messageStoreBootTimestamp = time.Now().UnixNano() / 1000000
	service.putMessageEntireTimeMax = 0
	service.getMessageEntireTimeMax = 0
	service.dispatchMaxBuffer = 0
	service.lastPrintTimestamp = time.Now().UnixNano() / 1000000

	for i := 0; i < len(service.putMessageDistributeTime); i++ {
		service.putMessageDistributeTime[i] = 0
	}

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
	for _, data := range self.putMessageTopicTimesTotal {
		atomic.AddInt64(&result, atomic.LoadInt64(&data))
	}

	return result
}

func (self *StoreStatsService) getFormatRuntime() string {
	var (
		MILLISECOND int64 = 1
		SECOND      int64 = 1000 * MILLISECOND
		MINUTE      int64 = 60 * SECOND
		HOUR        int64 = 60 * MINUTE
		DAY         int64 = 24 * HOUR
	)

	time := time.Now().UnixNano()/1000000 - self.messageStoreBootTimestamp
	days := time / DAY
	hours := (time % DAY) / HOUR
	minutes := (time % HOUR) / MINUTE
	seconds := (time % MINUTE) / SECOND

	result := fmt.Sprintf("[ %d days, %d hours, %d minutes, %d seconds ]", days, hours, minutes, seconds)
	return result
}

func (self *StoreStatsService) getPutMessageSizeTotal() int64 {
	result := int64(0)

	for _, value := range self.putMessageTopicSizeTotal {
		atomic.AddInt64(&result, value)
	}

	return result
}

func (self *StoreStatsService) getPutMessageDistributeTimeStringInfo(total int64) string {
	contentBuffer := bytes.Buffer{}

	for value := range self.putMessageDistributeTime {
		ratio := float64(value) / float64(total) * float64(100)
		contentBuffer.WriteString("\r\n\r\t")
		content := fmt.Sprintf("%d(%f%s)", value, ratio, "%")
		contentBuffer.WriteString(content)
	}

	return contentBuffer.String()
}

func (self *StoreStatsService) getPutTps() string {
	contentBuffer := bytes.Buffer{}
	// 10秒钟
	contentBuffer.WriteString(self.getPutTpsByTime(10))
	contentBuffer.WriteString(" ")

	// 1分钟
	contentBuffer.WriteString(self.getPutTpsByTime(60))
	contentBuffer.WriteString(" ")

	// 10分钟
	contentBuffer.WriteString(self.getPutTpsByTime(600))

	return contentBuffer.String()
}

func (self *StoreStatsService) getPutTpsByTime(time int) string {
	self.lockSampling.Lock()
	defer self.lockSampling.Unlock()

	result := ""
	lastElement := self.putTimesList.Back()

	if lastElement != nil {
		last, ok := lastElement.Value.(*CallSnapshot)
		if !ok {
			logger.Warn("store stats service get put tps by time type error")
			return result
		}

		if self.putTimesList.Len() > time {
			lastBefore := getCallSnapshotListByIndex(self.putTimesList, self.putTimesList.Len()-(time+1))
			if lastBefore != nil {
				result += strconv.FormatFloat(getTPS(lastBefore, last), 'E', -1, 64)
			}
		}
	}

	return result
}

func (self *StoreStatsService) getGetFoundTps() string {
	contentBuffer := bytes.Buffer{}
	// 10秒钟
	contentBuffer.WriteString(self.getGetFoundTpsByTime(10))
	contentBuffer.WriteString(" ")

	// 1分钟
	contentBuffer.WriteString(self.getGetFoundTpsByTime(60))
	contentBuffer.WriteString(" ")

	// 10分钟
	contentBuffer.WriteString(self.getGetFoundTpsByTime(600))

	return contentBuffer.String()
}

func (self *StoreStatsService) getGetFoundTpsByTime(time int) string {
	self.lockSampling.Lock()
	defer self.lockSampling.Unlock()

	result := ""
	lastElement := self.getTimesFoundList.Back()

	if lastElement != nil {
		last, ok := lastElement.Value.(*CallSnapshot)
		if !ok {
			logger.Warn("store stats service get put tps by time type error")
			return result
		}

		if self.getTimesFoundList.Len() > time {
			lastBefore := getCallSnapshotListByIndex(self.getTimesFoundList, self.getTimesFoundList.Len()-(time+1))
			if lastBefore != nil {
				result += strconv.FormatFloat(getTPS(lastBefore, last), 'E', -1, 64)
			}
		}
	}

	return result
}

func (self *StoreStatsService) getGetMissTps() string {
	contentBuffer := bytes.Buffer{}
	// 10秒钟
	contentBuffer.WriteString(self.getGetMissTpsByTime(10))
	contentBuffer.WriteString(" ")

	// 1分钟
	contentBuffer.WriteString(self.getGetMissTpsByTime(60))
	contentBuffer.WriteString(" ")

	// 10分钟
	contentBuffer.WriteString(self.getGetMissTpsByTime(600))

	return contentBuffer.String()
}

func (self *StoreStatsService) getGetMissTpsByTime(time int) string {
	self.lockSampling.Lock()
	defer self.lockSampling.Unlock()

	result := ""
	lastElement := self.getTimesMissList.Back()

	if lastElement != nil {
		last, ok := lastElement.Value.(*CallSnapshot)
		if !ok {
			logger.Warn("store stats service get put tps by time type error")
			return result
		}

		if self.getTimesMissList.Len() > time {
			lastBefore := getCallSnapshotListByIndex(self.getTimesMissList, self.getTimesMissList.Len()-(time+1))
			if lastBefore != nil {
				result += strconv.FormatFloat(getTPS(lastBefore, last), 'E', -1, 64)
			}
		}
	}

	return result
}

func (self *StoreStatsService) getGetTotalTps() string {
	contentBuffer := bytes.Buffer{}
	// 10秒钟
	contentBuffer.WriteString(self.getGetTotalTpsByTime(10))
	contentBuffer.WriteString(" ")

	// 1分钟
	contentBuffer.WriteString(self.getGetTotalTpsByTime(60))
	contentBuffer.WriteString(" ")

	// 10分钟
	contentBuffer.WriteString(self.getGetTotalTpsByTime(600))

	return contentBuffer.String()
}

func (self *StoreStatsService) getGetTotalTpsByTime(time int) string {
	self.lockSampling.Lock()
	defer self.lockSampling.Unlock()

	found := float64(0)
	miss := float64(0)
	lastElement := self.getTimesFoundList.Back()
	if lastElement == nil {
		return ""
	}

	last, ok := lastElement.Value.(*CallSnapshot)
	if !ok {
		logger.Warn("store stats service get put tps by time type error")
		return ""
	}

	if self.getTimesFoundList.Len() > time {
		lastBefore := getCallSnapshotListByIndex(self.getTimesFoundList, self.getTimesFoundList.Len()-(time+1))
		if lastBefore != nil {
			found = getTPS(lastBefore, last)
		}
	}

	lastElement = self.getTimesMissList.Back()
	if lastElement == nil {
		return ""
	}

	last, ok = lastElement.Value.(*CallSnapshot)
	if !ok {
		logger.Warn("store stats service get put tps by time type error")
		return ""
	}

	if self.getTimesMissList.Len() > time {
		lastBefore := getCallSnapshotListByIndex(self.getTimesMissList, self.getTimesMissList.Len()-(time+1))
		if lastBefore != nil {
			miss = getTPS(lastBefore, last)
		}
	}

	return strconv.FormatFloat(found+miss, 'E', -1, 64)
}

func (self *StoreStatsService) getGetTransferedTps() string {
	contentBuffer := bytes.Buffer{}
	// 10秒钟
	contentBuffer.WriteString(self.getGetTransferedTpsByTime(10))
	contentBuffer.WriteString(" ")

	// 1分钟
	contentBuffer.WriteString(self.getGetTransferedTpsByTime(60))
	contentBuffer.WriteString(" ")

	// 10分钟
	contentBuffer.WriteString(self.getGetTransferedTpsByTime(600))

	return contentBuffer.String()
}

func (self *StoreStatsService) getGetTransferedTpsByTime(time int) string {
	self.lockSampling.Lock()
	defer self.lockSampling.Unlock()

	result := ""
	lastElement := self.transferedMsgCountList.Back()

	if lastElement != nil {
		last, ok := lastElement.Value.(*CallSnapshot)
		if !ok {
			logger.Warn("store stats service get put tps by time type error")
			return result
		}

		if self.transferedMsgCountList.Len() > time {
			lastBefore := getCallSnapshotListByIndex(self.transferedMsgCountList, self.transferedMsgCountList.Len()-(time+1))
			if lastBefore != nil {
				result += strconv.FormatFloat(getTPS(lastBefore, last), 'E', -1, 64)
			}
		}
	}

	return result
}

func (self *StoreStatsService) GetRuntimeInfo() map[string]string {
	result := make(map[string]string)
	totalTimes := self.GetPutMessageTimesTotal()
	if 0 == totalTimes {
		totalTimes = 1
	}

	result["bootTimestamp"] = fmt.Sprintf("%d", self.messageStoreBootTimestamp)
	result["runtime"] = self.getFormatRuntime()
	result["putMessageEntireTimeMax"] = fmt.Sprintf("%d", self.putMessageEntireTimeMax)
	result["putMessageTimesTotal"] = fmt.Sprintf("%d", totalTimes)
	result["putMessageSizeTotal"] = fmt.Sprintf("%d", self.getPutMessageSizeTotal())
	result["putMessageDistributeTime"] = self.getPutMessageDistributeTimeStringInfo(totalTimes)
	result["putMessageAverageSize"] = fmt.Sprintf("%d", self.getPutMessageSizeTotal()/totalTimes)
	result["dispatchMaxBuffer"] = fmt.Sprintf("%d", self.dispatchMaxBuffer)
	result["getMessageEntireTimeMax"] = fmt.Sprintf("%d", self.getMessageEntireTimeMax)
	result["putTps"] = self.getPutTps()
	result["getFoundTps"] = self.getGetFoundTps()
	result["getMissTps"] = self.getGetMissTps()
	result["getTotalTps"] = self.getGetTotalTps()
	result["getTransferedTps"] = self.getGetTransferedTps()

	return result
}

func (self *StoreStatsService) setSinglePutMessageTopicSizeTotal(topic string, value int64) {
	self.putMessageTopicSizeTotal[topic] = value
}

func (self *StoreStatsService) getSinglePutMessageTopicSizeTotal(topic string) int64 {
	result, ok := self.putMessageTopicSizeTotal[topic]
	if !ok {
		result = int64(0)
		self.putMessageTopicSizeTotal[topic] = result
	}

	return result
}

func (self *StoreStatsService) setPutMessageEntireTimeMax(value int64) {
	if value <= 0 {
		atomic.AddInt64(&self.putMessageDistributeTime[0], 1)
	} else if value < 10 {
		atomic.AddInt64(&self.putMessageDistributeTime[1], 1)
	} else if value < 100 {
		atomic.AddInt64(&self.putMessageDistributeTime[2], 1)
	} else if value < 500 {
		atomic.AddInt64(&self.putMessageDistributeTime[3], 1)
	} else if value < 1000 {
		atomic.AddInt64(&self.putMessageDistributeTime[4], 1)
	} else if value < 10000 {
		atomic.AddInt64(&self.putMessageDistributeTime[5], 1)
	} else {
		atomic.AddInt64(&self.putMessageDistributeTime[6], 1)
	}

	if value > self.putMessageEntireTimeMax {
		self.lockPut.Lock()
		defer self.lockPut.Unlock()
		self.putMessageEntireTimeMax = value
	}
}

func (self *StoreStatsService) setSinglePutMessageTopicTimesTotal(topic string, value int64) {
	self.putMessageTopicTimesTotal[topic] = value
}

func (self *StoreStatsService) getSinglePutMessageTopicTimesTotal(topic string) int64 {
	result, ok := self.putMessageTopicTimesTotal[topic]
	if !ok {
		result = 0
		self.putMessageTopicTimesTotal[topic] = result
	}

	return result
}

func (self *StoreStatsService) setDispatchMaxBuffer(value int64) {
	if value > self.dispatchMaxBuffer {
		self.dispatchMaxBuffer = value
	}
}

func (self *StoreStatsService) Shutdown() {
	// TODO
}

type CallSnapshot struct {
	timestamp      int64
	callTimesTotal int64
}

func NewCallSnapshot(timestamp, callTimesTotal int64) *CallSnapshot {
	return &CallSnapshot{
		timestamp:      timestamp,
		callTimesTotal: callTimesTotal,
	}
}

func getTPS(begin, end *CallSnapshot) float64 {
	total := end.callTimesTotal - begin.callTimesTotal
	time := end.timestamp - begin.timestamp
	tps := float64(total) / float64(time)
	return tps * float64(1000)
}

func getCallSnapshotListByIndex(callSnapshotList *list.List, index int) *CallSnapshot {
	i := 0
	for e := callSnapshotList.Front(); e != nil; e = e.Next() {
		if index == i {
			callSnapshot := e.Value.(*CallSnapshot)
			return callSnapshot
		}

		i++
	}

	return nil
}

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
package stats

import (
	"bytes"
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/system"
)

type StoreStats interface {
	Start()
	Shutdown()
	GetSinglePutMessageTopicSizeTotal(topic string) int64
	SetSinglePutMessageTopicSizeTotal(topic string, value int64)
	GetSinglePutMessageTopicTimesTotal(topic string) int64
	SetSinglePutMessageTopicTimesTotal(topic string, value int64)
	SetPutMessageFailedTimes(value int64)
	SetMessageTransferedMsgCount(value int64)
	SetMessageTimesTotalFound(value int64)
	SetMessageTimesTotalMiss(value int64)
	SetMessageEntireTimeMax(value int64)
	SetPutMessageEntireTimeMax(value int64)
	SetDispatchMaxBuffer(value int64)
	GetPutMessageTimesTotal() int64
	GetGetMessageTransferedMsgCount() int64
	RuntimeInfo() map[string]string
}

const (
	FrequencyOfSampling  = 1000
	MaxRecordsOfSampling = 60 * 10
	PrintTPSInterval     = 60 * 1
)

type storeStatsService struct {
	putMessageFailedTimes     int64
	putMessageTopicTimesTotal map[string]int64
	putMessageTopicSizeTotal  map[string]int64
	messageTimesTotalFound    int64
	messageTransferedMsgCount int64
	messageTimesTotalMiss     int64
	putMessageDistributeTime  []int64
	putTimesList              *list.List
	timesFoundList            *list.List
	timesMissList             *list.List
	transferedMsgCountList    *list.List
	messageStoreBootTimestamp int64
	putMessageEntireTimeMax   int64
	messageEntireTimeMax      int64
	lockPut                   sync.Mutex
	lockGet                   sync.Mutex
	dispatchMaxBuffer         int64
	lockSampling              sync.Mutex
	lastPrintTimestamp        int64
	stop                      bool
	notify                    *system.WaitNotify
	timesMapMutex             sync.RWMutex
	sizeMapMutex              sync.RWMutex
}

func NewStoreStats() StoreStats {
	return newStoreStatsService()
}

func newStoreStatsService() *storeStatsService {
	service := new(storeStatsService)
	service.putMessageTopicTimesTotal = make(map[string]int64, 128)
	service.putMessageTopicSizeTotal = make(map[string]int64, 128)
	service.messageTimesTotalFound = 0
	service.messageTransferedMsgCount = 0
	service.messageTimesTotalMiss = 0
	service.putMessageDistributeTime = make([]int64, 7)
	service.putTimesList = list.New()
	service.timesFoundList = list.New()
	service.timesMissList = list.New()
	service.transferedMsgCountList = list.New()
	service.messageStoreBootTimestamp = system.CurrentTimeMillis()
	service.putMessageEntireTimeMax = 0
	service.messageEntireTimeMax = 0
	service.dispatchMaxBuffer = 0
	service.lastPrintTimestamp = system.CurrentTimeMillis()
	service.stop = false
	service.notify = system.NewWaitNotify()

	for i := 0; i < len(service.putMessageDistributeTime); i++ {
		service.putMessageDistributeTime[i] = 0
	}

	return service
}

func (service *storeStatsService) Start() {
	logger.Info("store stats service started.")

	for {
		if service.stop {
			break
		}

		service.notify.WaitForRunning(1000)
		service.sampling()
		service.printTps()
	}

	logger.Info("store stats service end.")
}

func (service *storeStatsService) sampling() {
	service.lockSampling.Lock()
	defer service.lockSampling.Unlock()

	service.putTimesList.PushBack(newCallSnapshot(system.CurrentTimeMillis(), service.GetPutMessageTimesTotal()))
	if service.putTimesList.Len() > MaxRecordsOfSampling+1 {
		service.putTimesList.Remove(service.putTimesList.Front())
	}

	service.timesFoundList.PushBack(newCallSnapshot(system.CurrentTimeMillis(),
		atomic.LoadInt64(&service.messageTimesTotalFound)))
	if service.timesFoundList.Len() > MaxRecordsOfSampling+1 {
		service.timesFoundList.Remove(service.timesFoundList.Front())
	}

	service.timesMissList.PushBack(newCallSnapshot(system.CurrentTimeMillis(),
		atomic.LoadInt64(&service.messageTimesTotalMiss)))
	if service.timesMissList.Len() > MaxRecordsOfSampling+1 {
		service.timesMissList.Remove(service.timesMissList.Front())
	}

	service.transferedMsgCountList.PushBack(newCallSnapshot(system.CurrentTimeMillis(),
		atomic.LoadInt64(&service.messageTransferedMsgCount)))
	if service.transferedMsgCountList.Len() > MaxRecordsOfSampling+1 {
		service.transferedMsgCountList.Remove(service.transferedMsgCountList.Front())
	}
}

func (service *storeStatsService) printTps() {
	if system.CurrentTimeMillis() > service.lastPrintTimestamp+PrintTPSInterval*1000 {
		service.lastPrintTimestamp = system.CurrentTimeMillis()

		logger.Info("put_tps ", service.getPutTpsByTime(PrintTPSInterval))
		logger.Info("get_found_tps ", service.getGetFoundTpsByTime(PrintTPSInterval))
		logger.Info("get_miss_tps ", service.getGetMissTpsByTime(PrintTPSInterval))
		logger.Info("get_transfered_tps ", service.getGetTransferedTpsByTime(PrintTPSInterval))
	}
}

func (service *storeStatsService) SetMessageEntireTimeMax(value int64) {
	if value > service.messageEntireTimeMax {
		service.lockGet.Lock()
		service.messageEntireTimeMax = value
		service.lockGet.Unlock()
	}
}

func (service *storeStatsService) GetGetMessageTransferedMsgCount() int64 {
	return atomic.LoadInt64(&service.messageTransferedMsgCount)
}

func (service *storeStatsService) GetPutMessageTimesTotal() int64 {
	service.timesMapMutex.RLock()
	defer service.timesMapMutex.RUnlock()

	result := int64(0)
	for _, data := range service.putMessageTopicTimesTotal {
		atomic.AddInt64(&result, atomic.LoadInt64(&data))
	}

	return result
}

func (service *storeStatsService) getFormatRuntime() string {
	var (
		MILLISECOND int64 = 1
		SECOND      int64 = 1000 * MILLISECOND
		MINUTE      int64 = 60 * SECOND
		HOUR        int64 = 60 * MINUTE
		DAY         int64 = 24 * HOUR
	)

	time := system.CurrentTimeMillis() - service.messageStoreBootTimestamp
	days := time / DAY
	hours := (time % DAY) / HOUR
	minutes := (time % HOUR) / MINUTE
	seconds := (time % MINUTE) / SECOND

	result := fmt.Sprintf("[ %d days, %d hours, %d minutes, %d seconds ]", days, hours, minutes, seconds)
	return result
}

func (service *storeStatsService) getPutMessageSizeTotal() int64 {
	service.sizeMapMutex.RLock()
	defer service.sizeMapMutex.RUnlock()

	result := int64(0)
	for _, value := range service.putMessageTopicSizeTotal {
		atomic.AddInt64(&result, value)
	}

	return result
}

func (service *storeStatsService) getPutMessageDistributeTimeStringInfo(total int64) string {
	contentBuffer := bytes.Buffer{}

	for value := range service.putMessageDistributeTime {
		ratio := float64(value) / float64(total) * float64(100)
		contentBuffer.WriteString("\r\n\r\t")
		content := fmt.Sprintf("%d(%f%s)", value, ratio, "%")
		contentBuffer.WriteString(content)
	}

	return contentBuffer.String()
}

func (service *storeStatsService) getPutTps() string {
	contentBuffer := bytes.Buffer{}
	// 10秒钟
	contentBuffer.WriteString(service.getPutTpsByTime(10))
	contentBuffer.WriteString(" ")

	// 1分钟
	contentBuffer.WriteString(service.getPutTpsByTime(60))
	contentBuffer.WriteString(" ")

	// 10分钟
	contentBuffer.WriteString(service.getPutTpsByTime(600))

	return contentBuffer.String()
}

func (service *storeStatsService) getPutTpsByTime(time int) string {
	service.lockSampling.Lock()
	defer service.lockSampling.Unlock()

	result := "0.00"
	lastElement := service.putTimesList.Back()

	if lastElement != nil {
		last, ok := lastElement.Value.(*callSnapshot)
		if !ok {
			logger.Warn("store stats service get put tps by time type error")
			return result
		}

		if service.putTimesList.Len() > time {
			lastBefore := getCallSnapshotListByIndex(service.putTimesList, service.putTimesList.Len()-(time+1))
			if lastBefore != nil {
				result = fmt.Sprintf("%0.2f", getTPS(lastBefore, last))
			}
		}
	}

	return result
}

func (service *storeStatsService) getGetFoundTps() string {
	contentBuffer := bytes.Buffer{}
	// 10秒钟
	contentBuffer.WriteString(service.getGetFoundTpsByTime(10))
	contentBuffer.WriteString(" ")

	// 1分钟
	contentBuffer.WriteString(service.getGetFoundTpsByTime(60))
	contentBuffer.WriteString(" ")

	// 10分钟
	contentBuffer.WriteString(service.getGetFoundTpsByTime(600))

	return contentBuffer.String()
}

func (service *storeStatsService) getGetFoundTpsByTime(time int) string {
	service.lockSampling.Lock()
	defer service.lockSampling.Unlock()

	result := "0.00"
	lastElement := service.timesFoundList.Back()

	if lastElement != nil {
		last, ok := lastElement.Value.(*callSnapshot)
		if !ok {
			logger.Warn("store stats service get put tps by time type error")
			return result
		}

		if service.timesFoundList.Len() > time {
			lastBefore := getCallSnapshotListByIndex(service.timesFoundList, service.timesFoundList.Len()-(time+1))
			if lastBefore != nil {
				result = fmt.Sprintf("%0.2f", getTPS(lastBefore, last))
			}
		}
	}

	return result
}

func (service *storeStatsService) getGetMissTps() string {
	contentBuffer := bytes.Buffer{}
	// 10秒钟
	contentBuffer.WriteString(service.getGetMissTpsByTime(10))
	contentBuffer.WriteString(" ")

	// 1分钟
	contentBuffer.WriteString(service.getGetMissTpsByTime(60))
	contentBuffer.WriteString(" ")

	// 10分钟
	contentBuffer.WriteString(service.getGetMissTpsByTime(600))

	return contentBuffer.String()
}

func (service *storeStatsService) getGetMissTpsByTime(time int) string {
	service.lockSampling.Lock()
	defer service.lockSampling.Unlock()

	result := "0.00"
	lastElement := service.timesMissList.Back()

	if lastElement != nil {
		last, ok := lastElement.Value.(*callSnapshot)
		if !ok {
			logger.Warn("store stats service get put tps by time type error")
			return result
		}

		if service.timesMissList.Len() > time {
			lastBefore := getCallSnapshotListByIndex(service.timesMissList, service.timesMissList.Len()-(time+1))
			if lastBefore != nil {
				result = fmt.Sprintf("%0.2f", getTPS(lastBefore, last))
			}
		}
	}

	return result
}

func (service *storeStatsService) getGetTotalTps() string {
	contentBuffer := bytes.Buffer{}
	// 10秒钟
	contentBuffer.WriteString(service.getGetTotalTpsByTime(10))
	contentBuffer.WriteString(" ")

	// 1分钟
	contentBuffer.WriteString(service.getGetTotalTpsByTime(60))
	contentBuffer.WriteString(" ")

	// 10分钟
	contentBuffer.WriteString(service.getGetTotalTpsByTime(600))

	return contentBuffer.String()
}

func (service *storeStatsService) getGetTotalTpsByTime(time int) string {
	service.lockSampling.Lock()
	defer service.lockSampling.Unlock()

	found := float64(0)
	miss := float64(0)
	lastElement := service.timesFoundList.Back()
	if lastElement == nil {
		return ""
	}

	last, ok := lastElement.Value.(*callSnapshot)
	if !ok {
		logger.Warn("store stats service get put tps by time type error")
		return ""
	}

	if service.timesFoundList.Len() > time {
		lastBefore := getCallSnapshotListByIndex(service.timesFoundList, service.timesFoundList.Len()-(time+1))
		if lastBefore != nil {
			found = getTPS(lastBefore, last)
		}
	}

	lastElement = service.timesMissList.Back()
	if lastElement == nil {
		return ""
	}

	last, ok = lastElement.Value.(*callSnapshot)
	if !ok {
		logger.Warn("store stats service get put tps by time type error")
		return ""
	}

	if service.timesMissList.Len() > time {
		lastBefore := getCallSnapshotListByIndex(service.timesMissList, service.timesMissList.Len()-(time+1))
		if lastBefore != nil {
			miss = getTPS(lastBefore, last)
		}
	}

	return fmt.Sprintf("%0.2f", found+miss)
}

func (service *storeStatsService) getGetTransferedTps() string {
	contentBuffer := bytes.Buffer{}
	// 10秒钟
	contentBuffer.WriteString(service.getGetTransferedTpsByTime(10))
	contentBuffer.WriteString(" ")

	// 1分钟
	contentBuffer.WriteString(service.getGetTransferedTpsByTime(60))
	contentBuffer.WriteString(" ")

	// 10分钟
	contentBuffer.WriteString(service.getGetTransferedTpsByTime(600))

	return contentBuffer.String()
}

func (service *storeStatsService) getGetTransferedTpsByTime(time int) string {
	service.lockSampling.Lock()
	defer service.lockSampling.Unlock()

	result := "0.00"
	lastElement := service.transferedMsgCountList.Back()

	if lastElement != nil {
		last, ok := lastElement.Value.(*callSnapshot)
		if !ok {
			logger.Warn("store stats service get put tps by time type error")
			return result
		}

		if service.transferedMsgCountList.Len() > time {
			lastBefore := getCallSnapshotListByIndex(service.transferedMsgCountList, service.transferedMsgCountList.Len()-(time+1))
			if lastBefore != nil {
				result = fmt.Sprintf("%0.2f", getTPS(lastBefore, last))
			}
		}
	}

	return result
}

func (service *storeStatsService) RuntimeInfo() map[string]string {
	result := make(map[string]string)
	totalTimes := service.GetPutMessageTimesTotal()
	if 0 == totalTimes {
		totalTimes = 1
	}

	result["bootTimestamp"] = fmt.Sprintf("%d", service.messageStoreBootTimestamp)
	result["runtime"] = service.getFormatRuntime()
	result["putMessageEntireTimeMax"] = fmt.Sprintf("%d", service.putMessageEntireTimeMax)
	result["putMessageTimesTotal"] = fmt.Sprintf("%d", totalTimes)
	result["putMessageSizeTotal"] = fmt.Sprintf("%d", service.getPutMessageSizeTotal())
	result["putMessageDistributeTime"] = service.getPutMessageDistributeTimeStringInfo(totalTimes)
	result["putMessageAverageSize"] = fmt.Sprintf("%d", service.getPutMessageSizeTotal()/totalTimes)
	result["dispatchMaxBuffer"] = fmt.Sprintf("%d", service.dispatchMaxBuffer)
	result["messageEntireTimeMax"] = fmt.Sprintf("%d", service.messageEntireTimeMax)
	result["putTps"] = service.getPutTps()
	result["getFoundTps"] = service.getGetFoundTps()
	result["getMissTps"] = service.getGetMissTps()
	result["getTotalTps"] = service.getGetTotalTps()
	result["getTransferedTps"] = service.getGetTransferedTps()

	return result
}

func (service *storeStatsService) SetSinglePutMessageTopicSizeTotal(topic string, value int64) {
	service.sizeMapMutex.Lock()
	defer service.sizeMapMutex.Unlock()

	service.putMessageTopicSizeTotal[topic] = value
}

func (service *storeStatsService) GetSinglePutMessageTopicSizeTotal(topic string) int64 {
	service.sizeMapMutex.Lock()
	defer service.sizeMapMutex.Unlock()

	result, ok := service.putMessageTopicSizeTotal[topic]
	if !ok {
		result = int64(0)
		service.putMessageTopicSizeTotal[topic] = result
	}

	return result
}

func (service *storeStatsService) SetPutMessageEntireTimeMax(value int64) {
	if value <= 0 {
		atomic.AddInt64(&service.putMessageDistributeTime[0], 1)
	} else if value < 10 {
		atomic.AddInt64(&service.putMessageDistributeTime[1], 1)
	} else if value < 100 {
		atomic.AddInt64(&service.putMessageDistributeTime[2], 1)
	} else if value < 500 {
		atomic.AddInt64(&service.putMessageDistributeTime[3], 1)
	} else if value < 1000 {
		atomic.AddInt64(&service.putMessageDistributeTime[4], 1)
	} else if value < 10000 {
		atomic.AddInt64(&service.putMessageDistributeTime[5], 1)
	} else {
		atomic.AddInt64(&service.putMessageDistributeTime[6], 1)
	}

	if value > service.putMessageEntireTimeMax {
		service.lockPut.Lock()
		defer service.lockPut.Unlock()
		service.putMessageEntireTimeMax = value
	}
}

func (service *storeStatsService) SetSinglePutMessageTopicTimesTotal(topic string, value int64) {
	service.timesMapMutex.Lock()
	defer service.timesMapMutex.Unlock()

	service.putMessageTopicTimesTotal[topic] = value
}

func (service *storeStatsService) GetSinglePutMessageTopicTimesTotal(topic string) int64 {
	service.timesMapMutex.Lock()
	defer service.timesMapMutex.Unlock()

	result, ok := service.putMessageTopicTimesTotal[topic]
	if !ok {
		result = 0
		service.putMessageTopicTimesTotal[topic] = result
	}

	return result
}

func (service *storeStatsService) SetDispatchMaxBuffer(value int64) {
	if value > service.dispatchMaxBuffer {
		service.dispatchMaxBuffer = value
	}
}

func (service *storeStatsService) SetPutMessageFailedTimes(value int64) {
	atomic.AddInt64(&service.putMessageFailedTimes, value)
}

func (service *storeStatsService) SetMessageTransferedMsgCount(value int64) {
	atomic.AddInt64(&service.messageTransferedMsgCount, value)
}

func (service *storeStatsService) SetMessageTimesTotalFound(value int64) {
	atomic.AddInt64(&service.messageTimesTotalFound, value)
}

func (service *storeStatsService) SetMessageTimesTotalMiss(value int64) {
	atomic.AddInt64(&service.messageTimesTotalMiss, value)
}

func (service *storeStatsService) Shutdown() {
	service.stop = true
}

func getTPS(begin, end *callSnapshot) float64 {
	total := end.times - begin.times
	time := end.timestamp - begin.timestamp
	tps := float64(total) / float64(time)
	return tps * float64(1000)
}

func getCallSnapshotListByIndex(csList *list.List, index int) *callSnapshot {
	i := 0
	for e := csList.Front(); e != nil; e = e.Next() {
		if index == i {
			cs := e.Value.(*callSnapshot)
			return cs
		}

		i++
	}

	return nil
}

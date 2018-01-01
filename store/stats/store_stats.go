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
	GetSinglePutMessageTopicSizeTotal(topic string) int64
	SetSinglePutMessageTopicSizeTotal(topic string, value int64)
	SetDispatchMaxBuffer(value int64)
	GetSinglePutMessageTopicTimesTotal(topic string) int64
}

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
	lockPut                      sync.Mutex
	lockGet                      sync.Mutex
	dispatchMaxBuffer            int64
	lockSampling                 sync.Mutex
	lastPrintTimestamp           int64
	stop                         bool
	notify                       *WaitNotifyObject
	timesMapMutex                sync.RWMutex
	sizeMapMutex                 sync.RWMutex
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
	service.messageStoreBootTimestamp = system.CurrentTimeMillis()
	service.putMessageEntireTimeMax = 0
	service.getMessageEntireTimeMax = 0
	service.dispatchMaxBuffer = 0
	service.lastPrintTimestamp = system.CurrentTimeMillis()
	service.stop = false
	service.notify = NewWaitNotifyObject()

	for i := 0; i < len(service.putMessageDistributeTime); i++ {
		service.putMessageDistributeTime[i] = 0
	}

	return service
}

func (service *StoreStatsService) Start() {
	logger.Info("store stats service started")

	for {
		if service.stop {
			break
		}

		service.notify.waitForRunning(1000)
		service.sampling()
		service.printTps()
	}

	logger.Info("store stats service end")
}

func (service *StoreStatsService) sampling() {
	service.lockSampling.Lock()
	defer service.lockSampling.Unlock()

	service.putTimesList.PushBack(newCallSnapshot(system.CurrentTimeMillis(), service.GetPutMessageTimesTotal()))
	if service.putTimesList.Len() > MaxRecordsOfSampling+1 {
		service.putTimesList.Remove(service.putTimesList.Front())
	}

	service.getTimesFoundList.PushBack(newCallSnapshot(system.CurrentTimeMillis(),
		atomic.LoadInt64(&service.getMessageTimesTotalFound)))
	if service.getTimesFoundList.Len() > MaxRecordsOfSampling+1 {
		service.getTimesFoundList.Remove(service.getTimesFoundList.Front())
	}

	service.getTimesMissList.PushBack(newCallSnapshot(system.CurrentTimeMillis(),
		atomic.LoadInt64(&service.getMessageTimesTotalMiss)))
	if service.getTimesMissList.Len() > MaxRecordsOfSampling+1 {
		service.getTimesMissList.Remove(service.getTimesMissList.Front())
	}

	service.transferedMsgCountList.PushBack(newCallSnapshot(system.CurrentTimeMillis(),
		atomic.LoadInt64(&service.getMessageTransferedMsgCount)))
	if service.transferedMsgCountList.Len() > MaxRecordsOfSampling+1 {
		service.transferedMsgCountList.Remove(service.transferedMsgCountList.Front())
	}
}

func (service *StoreStatsService) printTps() {
	if system.CurrentTimeMillis() > service.lastPrintTimestamp+PrintTPSInterval*1000 {
		service.lastPrintTimestamp = system.CurrentTimeMillis()

		logger.Info("put_tps ", service.getPutTpsByTime(PrintTPSInterval))
		logger.Info("get_found_tps ", service.getGetFoundTpsByTime(PrintTPSInterval))
		logger.Info("get_miss_tps ", service.getGetMissTpsByTime(PrintTPSInterval))
		logger.Info("get_transfered_tps ", service.getGetTransferedTpsByTime(PrintTPSInterval))
	}
}

func (service *StoreStatsService) SetGetMessageEntireTimeMax(value int64) {
	if value > service.getMessageEntireTimeMax {
		service.lockGet.Lock()
		service.getMessageEntireTimeMax = value
		service.lockGet.Unlock()
	}
}

func (service *StoreStatsService) GetGetMessageTransferedMsgCount() int64 {
	return atomic.LoadInt64(&service.getMessageTransferedMsgCount)
}

func (service *StoreStatsService) GetPutMessageTimesTotal() int64 {
	service.timesMapMutex.RLock()
	defer service.timesMapMutex.RUnlock()

	result := int64(0)
	for _, data := range service.putMessageTopicTimesTotal {
		atomic.AddInt64(&result, atomic.LoadInt64(&data))
	}

	return result
}

func (service *StoreStatsService) getFormatRuntime() string {
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

func (service *StoreStatsService) getPutMessageSizeTotal() int64 {
	service.sizeMapMutex.RLock()
	defer service.sizeMapMutex.RUnlock()

	result := int64(0)
	for _, value := range service.putMessageTopicSizeTotal {
		atomic.AddInt64(&result, value)
	}

	return result
}

func (service *StoreStatsService) getPutMessageDistributeTimeStringInfo(total int64) string {
	contentBuffer := bytes.Buffer{}

	for value := range service.putMessageDistributeTime {
		ratio := float64(value) / float64(total) * float64(100)
		contentBuffer.WriteString("\r\n\r\t")
		content := fmt.Sprintf("%d(%f%s)", value, ratio, "%")
		contentBuffer.WriteString(content)
	}

	return contentBuffer.String()
}

func (service *StoreStatsService) getPutTps() string {
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

func (service *StoreStatsService) getPutTpsByTime(time int) string {
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

func (service *StoreStatsService) getGetFoundTps() string {
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

func (service *StoreStatsService) getGetFoundTpsByTime(time int) string {
	service.lockSampling.Lock()
	defer service.lockSampling.Unlock()

	result := "0.00"
	lastElement := service.getTimesFoundList.Back()

	if lastElement != nil {
		last, ok := lastElement.Value.(*callSnapshot)
		if !ok {
			logger.Warn("store stats service get put tps by time type error")
			return result
		}

		if service.getTimesFoundList.Len() > time {
			lastBefore := getCallSnapshotListByIndex(service.getTimesFoundList, service.getTimesFoundList.Len()-(time+1))
			if lastBefore != nil {
				result = fmt.Sprintf("%0.2f", getTPS(lastBefore, last))
			}
		}
	}

	return result
}

func (service *StoreStatsService) getGetMissTps() string {
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

func (service *StoreStatsService) getGetMissTpsByTime(time int) string {
	service.lockSampling.Lock()
	defer service.lockSampling.Unlock()

	result := "0.00"
	lastElement := service.getTimesMissList.Back()

	if lastElement != nil {
		last, ok := lastElement.Value.(*callSnapshot)
		if !ok {
			logger.Warn("store stats service get put tps by time type error")
			return result
		}

		if service.getTimesMissList.Len() > time {
			lastBefore := getCallSnapshotListByIndex(service.getTimesMissList, service.getTimesMissList.Len()-(time+1))
			if lastBefore != nil {
				result = fmt.Sprintf("%0.2f", getTPS(lastBefore, last))
			}
		}
	}

	return result
}

func (service *StoreStatsService) getGetTotalTps() string {
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

func (service *StoreStatsService) getGetTotalTpsByTime(time int) string {
	service.lockSampling.Lock()
	defer service.lockSampling.Unlock()

	found := float64(0)
	miss := float64(0)
	lastElement := service.getTimesFoundList.Back()
	if lastElement == nil {
		return ""
	}

	last, ok := lastElement.Value.(*callSnapshot)
	if !ok {
		logger.Warn("store stats service get put tps by time type error")
		return ""
	}

	if service.getTimesFoundList.Len() > time {
		lastBefore := getCallSnapshotListByIndex(service.getTimesFoundList, service.getTimesFoundList.Len()-(time+1))
		if lastBefore != nil {
			found = getTPS(lastBefore, last)
		}
	}

	lastElement = service.getTimesMissList.Back()
	if lastElement == nil {
		return ""
	}

	last, ok = lastElement.Value.(*callSnapshot)
	if !ok {
		logger.Warn("store stats service get put tps by time type error")
		return ""
	}

	if service.getTimesMissList.Len() > time {
		lastBefore := getCallSnapshotListByIndex(service.getTimesMissList, service.getTimesMissList.Len()-(time+1))
		if lastBefore != nil {
			miss = getTPS(lastBefore, last)
		}
	}

	return fmt.Sprintf("%0.2f", found+miss)
}

func (service *StoreStatsService) getGetTransferedTps() string {
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

func (service *StoreStatsService) getGetTransferedTpsByTime(time int) string {
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

func (service *StoreStatsService) GetRuntimeInfo() map[string]string {
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
	result["getMessageEntireTimeMax"] = fmt.Sprintf("%d", service.getMessageEntireTimeMax)
	result["putTps"] = service.getPutTps()
	result["getFoundTps"] = service.getGetFoundTps()
	result["getMissTps"] = service.getGetMissTps()
	result["getTotalTps"] = service.getGetTotalTps()
	result["getTransferedTps"] = service.getGetTransferedTps()

	return result
}

func (service *StoreStatsService) SetSinglePutMessageTopicSizeTotal(topic string, value int64) {
	service.sizeMapMutex.Lock()
	defer service.sizeMapMutex.Unlock()

	service.putMessageTopicSizeTotal[topic] = value
}

func (service *StoreStatsService) GetSinglePutMessageTopicSizeTotal(topic string) int64 {
	service.sizeMapMutex.Lock()
	defer service.sizeMapMutex.Unlock()

	result, ok := service.putMessageTopicSizeTotal[topic]
	if !ok {
		result = int64(0)
		service.putMessageTopicSizeTotal[topic] = result
	}

	return result
}

func (service *StoreStatsService) setPutMessageEntireTimeMax(value int64) {
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

func (service *StoreStatsService) setSinglePutMessageTopicTimesTotal(topic string, value int64) {
	service.timesMapMutex.Lock()
	defer service.timesMapMutex.Unlock()

	service.putMessageTopicTimesTotal[topic] = value
}

func (service *StoreStatsService) GetSinglePutMessageTopicTimesTotal(topic string) int64 {
	service.timesMapMutex.Lock()
	defer service.timesMapMutex.Unlock()

	result, ok := service.putMessageTopicTimesTotal[topic]
	if !ok {
		result = 0
		service.putMessageTopicTimesTotal[topic] = result
	}

	return result
}

func (service *StoreStatsService) SetDispatchMaxBuffer(value int64) {
	if value > service.dispatchMaxBuffer {
		service.dispatchMaxBuffer = value
	}
}

func (service *StoreStatsService) Shutdown() {
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

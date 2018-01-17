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
package persistent

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/boltmq/store"
	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/sysflag"
	"github.com/boltmq/common/utils/codec"
	"github.com/boltmq/common/utils/system"
)

const (
	MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8
	BlankMagicCode   = 0xBBCCDDEE ^ 1880681586 + 8
)

type commitLog struct {
	mfq               *mappedFileQueue
	messageStore      *PersistentMessageStore
	flushCLogService  flushCommitLogService
	appendMsgCallback appendMessageCallback
	topicQueueTable   map[string]int64
	mutex             sync.Mutex
}

func newCommitLog(messageStore *PersistentMessageStore) *commitLog {
	clog := &commitLog{}
	clog.mfq = newMappedFileQueue(messageStore.config.StorePathCommitLog,
		int64(messageStore.config.MappedFileSizeCommitLog), messageStore.allocateMFileService)
	clog.messageStore = messageStore

	if SYNC_FLUSH == messageStore.config.FlushDisk {
		clog.flushCLogService = newGroupCommitService(clog)
	} else {
		clog.flushCLogService = newFlushRealTimeService(clog)
	}

	clog.topicQueueTable = make(map[string]int64, 1024)
	clog.appendMsgCallback = newDefaultAppendMessageCallback(messageStore.config.MaxMessageSize, clog)
	return clog
}

func (clog *commitLog) load() bool {
	result := clog.mfq.load()
	if result {
		logger.Info("load commitlog completed.")
	} else {
		logger.Error("load commitlog failed.")
	}

	return result
}

func (clog *commitLog) putMessage(msg *store.MessageExtInner) *store.PutMessageResult {
	msg.StoreTimestamp = system.CurrentTimeMillis()
	msg.BodyCRC, _ = codec.Crc32(msg.Body)

	// TODO 事务消息处理
	clog.mutex.Lock()
	beginLockTimestamp := system.CurrentTimeMillis()
	msg.BornTimestamp = beginLockTimestamp

	mf, err := clog.mfq.getLastMappedFile(int64(0))
	if err != nil {
		// TODO
		return &store.PutMessageResult{Status: store.CREATE_MAPPED_FILE_FAILED}
	}

	if mf == nil {
		// TODO
		return &store.PutMessageResult{Status: store.CREATE_MAPPED_FILE_FAILED}
	}

	result := mf.appendMessageWithCallBack(msg, clog.appendMsgCallback)
	switch result.Status {
	case store.APPENDMESSAGE_PUT_OK:
		break
	case store.END_OF_FILE:
		mf, err = clog.mfq.getLastMappedFile(int64(0))
		if err != nil {
			logger.Errorf("put message get last mapped file err: %s.", err)
			return &store.PutMessageResult{Status: store.CREATE_MAPPED_FILE_FAILED, Result: result}
		}

		if mf == nil {
			logger.Errorf("create mapped file2 error, topic:%s clientAddr:%s.", msg.Topic, msg.BornHost)
			return &store.PutMessageResult{Status: store.CREATE_MAPPED_FILE_FAILED, Result: result}
		}

		result = mf.appendMessageWithCallBack(msg, clog.appendMsgCallback)
		break
	case store.MESSAGE_SIZE_EXCEEDED:
		return &store.PutMessageResult{Status: store.MESSAGE_ILLEGAL, Result: result}
	default:
		return &store.PutMessageResult{Status: store.PUTMESSAGE_UNKNOWN_ERROR, Result: result}
	}

	// dispatchRequest
	disRequest := &dispatchRequest{
		topic:                     msg.Topic,
		queueId:                   msg.QueueId,
		commitLogOffset:           result.WroteOffset,
		msgSize:                   result.WroteBytes,
		tagsCode:                  msg.TagsCode,
		storeTimestamp:            msg.StoreTimestamp,
		consumeQueueOffset:        result.LogicsOffset,
		keys:                      msg.GetKeys(),
		sysFlag:                   msg.SysFlag,
		tranStateTableOffset:      msg.QueueOffset,
		preparedTransactionOffset: msg.PreparedTransactionOffset,
		producerGroup:             message.PROPERTY_PRODUCER_GROUP,
	}

	clog.messageStore.dispatchMsgService.putRequest(disRequest)

	eclipseTimeInLock := system.CurrentTimeMillis() - beginLockTimestamp
	clog.mutex.Unlock()

	if eclipseTimeInLock > 1000 {
		logger.Warnf("putMessage in lock eclipse time(ms) %d.", eclipseTimeInLock)
	}

	putMessageResult := &store.PutMessageResult{Status: store.PUTMESSAGE_PUT_OK, Result: result}

	// Statistics
	size := clog.messageStore.storeStats.GetSinglePutMessageTopicSizeTotal(msg.Topic)
	clog.messageStore.storeStats.SetSinglePutMessageTopicSizeTotal(msg.Topic, atomic.AddInt64(&size, result.WroteBytes))

	// Synchronization flush
	if SYNC_FLUSH == clog.messageStore.config.FlushDisk {
		if msg.IsWaitStoreMsgOK() {
			if gcService, ok := clog.flushCLogService.(*groupCommitService); ok {
				request := newGroupCommitRequest(result.WroteOffset + result.WroteBytes)
				gcService.putRequest(request)

				flushOk := request.waitForFlush(int64(clog.messageStore.config.SyncFlushTimeout))
				if flushOk == false {
					logger.Errorf("do groupcommit, wait for flush failed, topic: %s tags: %s client address: %s.",
						msg.Topic, msg.GetTags(), msg.BornHost)
					putMessageResult.Status = store.FLUSH_DISK_TIMEOUT
				}
			}
		}
	} else {
		if flushRTimeService, ok := clog.flushCLogService.(*flushRealTimeService); ok {
			flushRTimeService.wakeup()
		}
	}

	// Synchronous write double
	if SYNC_MASTER == clog.messageStore.config.BrokerRole {
		// TODO
	}

	return putMessageResult
}

func (clog *commitLog) getMessage(offset int64, size int32) *mappedBufferResult {
	returnFirstOnNotFound := false
	if 0 == offset {
		returnFirstOnNotFound = true
	}

	mf := clog.mfq.findMappedFileByOffset(offset, returnFirstOnNotFound)
	if mf != nil {
		mappedFileSize := clog.messageStore.config.MappedFileSizeCommitLog
		pos := offset % int64(mappedFileSize)
		result := mf.selectMappedBufferByPosAndSize(pos, size)
		return result
	}

	return nil
}

func (clog *commitLog) rollNextFile(offset int64) int64 {
	mappedFileSize := clog.messageStore.config.MappedFileSizeCommitLog
	nextOffset := offset + int64(mappedFileSize) - offset%int64(mappedFileSize)
	return nextOffset
}

func (clog *commitLog) recoverNormally() {
	checkCRCOnRecover := clog.messageStore.config.CheckCRCOnRecover
	mappedFiles := clog.mfq.mappedFiles
	if mappedFiles != nil && mappedFiles.Len() > 0 {
		index := mappedFiles.Len() - 3
		if index < 0 {
			index = 0
		}

		mf := getMappedFileByIndex(mappedFiles, index)
		byteBuffer := newMappedByteBuffer(mf.byteBuffer.Bytes())
		byteBuffer.writePos = mf.byteBuffer.writePos
		processOffset := mf.fileFromOffset
		mappedFileOffset := int64(0)
		for {
			disRequest := clog.checkMessageAndReturnSize(byteBuffer,
				clog.messageStore.config.CheckCRCOnRecover, checkCRCOnRecover)
			size := disRequest.msgSize
			if size > 0 {
				mappedFileOffset += size
			} else if size == -1 {
				logger.Infof("recover physics file end, %s.", mf.fileName)
				break
			} else if size == 0 {
				index++
				if index >= mappedFiles.Len() {
					logger.Infof("recover last 3 physics file over, last mapped file %s.", mf.fileName)
					break
				} else {
					mf = getMappedFileByIndex(mappedFiles, index)
					byteBuffer = mf.byteBuffer
					byteBuffer.writePos = mf.byteBuffer.writePos
					processOffset = mf.fileFromOffset
					mappedFileOffset = 0
				}
			}
		}

		processOffset += mappedFileOffset
		clog.mfq.committedWhere = processOffset
		clog.mfq.truncateDirtyFiles(processOffset)

	}
}

func (clog *commitLog) pickupStoretimestamp(offset int64, size int32) int64 {
	if offset > clog.getMinOffset() {
		result := clog.getMessage(offset, size)
		if result != nil {
			defer result.Release()
			result.byteBuffer.readPos = message.MessageStoreTimestampPostion
			storeTimestamp := result.byteBuffer.ReadInt64()
			return storeTimestamp
		}
	}

	return -1
}

func (clog *commitLog) getMinOffset() int64 {
	mf := clog.mfq.getFirstMappedFileOnLock()
	if mf != nil {
		// TODO mf.isAvailable()
		return mf.fileFromOffset
	}

	return -1
}

func (clog *commitLog) getMaxOffset() int64 {
	return clog.mfq.getMaxOffset()
}

func (clog *commitLog) removeQueurFromTopicQueueTable(topic string, queueId int32) {
	clog.mutex.Lock()
	clog.mutex.Unlock()
	key := fmt.Sprintf("%s-%d", topic, queueId)
	delete(clog.topicQueueTable, key)
}

func (clog *commitLog) checkMessageAndReturnSize(byteBuffer *mappedByteBuffer, checkCRC bool, readBody bool) *dispatchRequest {
	messageMagicCode := MessageMagicCode
	blankMagicCode := BlankMagicCode

	bufferLen := byteBuffer.writePos - byteBuffer.readPos
	totalSize := byteBuffer.ReadInt32() // 1 TOTALSIZE
	magicCode := byteBuffer.ReadInt32() // 2 MAGICCODE

	switch magicCode {
	case int32(messageMagicCode):
		break
	case int32(blankMagicCode):
		return &dispatchRequest{msgSize: 0}
	default:
		logger.Warnf("found a illegal magic code message magic code:%d, blank magic code:%d, current magic code:%d.",
			messageMagicCode, blankMagicCode, magicCode)
		return &dispatchRequest{msgSize: -1}
	}

	if totalSize > int32(bufferLen) {
		return &dispatchRequest{msgSize: -1}
	}

	bodyCRC := byteBuffer.ReadInt32()                   // 3 BODYCRC
	queueId := byteBuffer.ReadInt32()                   // 4 QUEUEID
	byteBuffer.ReadInt32()                              // 5 FLAG
	queueOffset := byteBuffer.ReadInt64()               // 6 QUEUEOFFSET
	physicOffset := byteBuffer.ReadInt64()              // 7 PHYSICALOFFSET
	sysFlag := byteBuffer.ReadInt32()                   // 8 SYSFLAG
	byteBuffer.ReadInt64()                              // 9 BORNTIMESTAMP
	byteBuffer.Read(make([]byte, 8))                    // 10 BORNHOST（IP+PORT）
	storeTimestamp := byteBuffer.ReadInt64()            // 11 STORETIMESTAMP
	byteBuffer.Read(make([]byte, 8))                    // 12 STOREHOST（IP+PORT）
	byteBuffer.ReadInt32()                              // 13 RECONSUMETIMES
	preparedTransactionOffset := byteBuffer.ReadInt64() // 14 Prepared Transaction Offset
	// 15 BODY
	bodyLen := byteBuffer.ReadInt32()
	if bodyLen > 0 {
		if readBody {
			bodyContent := make([]byte, bodyLen)
			byteBuffer.Read(bodyContent)
			if checkCRC {
				crc, _ := codec.Crc32(bodyContent)
				if int32(crc) != bodyCRC {
					logger.Warnf("CRC check failed crc:%d, bodyCRC:%d.", crc, bodyCRC)
					return &dispatchRequest{msgSize: -1}
				}
			}
		} else {
			byteBuffer.readPos = byteBuffer.readPos + int(bodyLen)
		}
	}

	var (
		topic          = ""
		keys           = ""
		tagsCode int64 = 0
	)

	// 16 TOPIC
	topicLen := byteBuffer.ReadInt8()
	if topicLen > 0 {
		topicBytes := make([]byte, topicLen)
		byteBuffer.Read(topicBytes)
		topic = string(topicBytes)
	}

	// 17 properties
	propertiesLength := byteBuffer.ReadInt16()
	if propertiesLength > 0 {
		propertiesBytes := make([]byte, propertiesLength)
		byteBuffer.Read(propertiesBytes)
		properties := string(propertiesBytes)
		propertiesMap := message.String2messageProperties(properties)
		keys = propertiesMap[message.PROPERTY_KEYS]
		tags := propertiesMap[message.PROPERTY_TAGS]
		if len(tags) > 0 {
			tagsCode = basis.TagsString2tagsCode(basis.ParseTopicFilterType(sysFlag), tags)
		}

		// Timing message processing
		delayLevelStr, ok := propertiesMap[message.PROPERTY_DELAY_TIME_LEVEL]
		if SCHEDULE_TOPIC == topic && ok {
			delayLevelTemp, _ := strconv.Atoi(delayLevelStr)
			delayLevel := int32(delayLevelTemp)

			if delayLevel > clog.messageStore.scheduleMsgService.maxDelayLevel {
				delayLevel = clog.messageStore.scheduleMsgService.maxDelayLevel
			}

			if delayLevel > 0 {
				tagsCode = clog.messageStore.scheduleMsgService.computeDeliverTimestamp(delayLevel, storeTimestamp)
			}
		}
	}

	return &dispatchRequest{
		topic:                     topic,                     // 1
		queueId:                   queueId,                   // 2
		commitLogOffset:           physicOffset,              // 3
		msgSize:                   int64(totalSize),          // 4
		tagsCode:                  tagsCode,                  // 5
		storeTimestamp:            storeTimestamp,            // 6
		consumeQueueOffset:        queueOffset,               // 7
		keys:                      keys,                      // 8
		sysFlag:                   sysFlag,                   // 9
		tranStateTableOffset:      int64(0),                  // 10
		preparedTransactionOffset: preparedTransactionOffset, // 11
		producerGroup:             "",                        // 12
	}
}

func (clog *commitLog) recoverAbnormally() {
	checkCRCOnRecover := clog.messageStore.config.CheckCRCOnRecover
	mappedFiles := clog.mfq.mappedFiles
	if mappedFiles.Len() > 0 {
		// Looking beginning to recover from which file
		var mf *mappedFile
		index := mappedFiles.Len() - 1

		for element := mappedFiles.Back(); element != nil; element = element.Prev() {
			index--
			mf = element.Value.(*mappedFile)
			if clog.isMappedFileMatchedRecover(mf) {
				logger.Info("recover from this mapped file ", mf.fileName)
				break
			}
		}

		if mf != nil {
			mappedByteBuffer := newMappedByteBuffer(mf.byteBuffer.Bytes())
			mappedByteBuffer.writePos = mf.byteBuffer.writePos
			processOffset := mf.fileFromOffset
			mappedFileOffset := int64(0)

			for {
				disRequest := clog.checkMessageAndReturnSize(mappedByteBuffer, checkCRCOnRecover, true)
				size := disRequest.msgSize

				if size > 0 { // Normal data
					mappedFileOffset += size
					clog.messageStore.putDispatchRequest(disRequest)
				} else if size == -1 { // Intermediate file read error
					logger.Infof("recover physics file end, %s.", mf.fileName)
					break
				} else if size == 0 {
					index++

					if index >= mappedFiles.Len() { // The current branch under normal circumstances should
						logger.Infof("recover physics file over, last mapped file %s.", mf.fileName)
						break
					} else {
						i := 0
						for element := mappedFiles.Front(); element != nil; element = element.Next() {
							if i == index {
								mf = element.Value.(*mappedFile)
								mappedByteBuffer = newMappedByteBuffer(mf.byteBuffer.Bytes())
								mappedByteBuffer.writePos = mf.byteBuffer.writePos
								processOffset = mf.fileFromOffset
								mappedFileOffset = 0
								logger.Infof("recover next physics file, %s.", mf.fileName)
								break
							}

							i++
						}
					}
				}
			}

			processOffset += mappedFileOffset
			clog.mfq.committedWhere = processOffset
			clog.mfq.truncateDirtyFiles(processOffset)

			// Clear ConsumeQueue redundant data
			clog.messageStore.truncateDirtyLogicFiles(processOffset)
		} else {
			// Commitlog case files are deleted
			clog.mfq.committedWhere = 0
			clog.messageStore.destroyLogics()
		}
	}
}

func (clog *commitLog) isMappedFileMatchedRecover(mf *mappedFile) bool {
	bytes := mf.byteBuffer.Bytes()
	if len(bytes) == 0 {
		return false
	}

	byteBuffer := newMappedByteBuffer(bytes)
	byteBuffer.writePos = mf.byteBuffer.writePos
	byteBuffer.readPos = message.MessageMagicCodePostion
	magicCode := byteBuffer.ReadInt32()
	messageMagicCode := MessageMagicCode
	if magicCode != int32(messageMagicCode) {
		return false
	}

	byteBuffer.readPos = message.MessageStoreTimestampPostion
	storeTimestamp := byteBuffer.ReadInt64()
	if 0 == storeTimestamp {
		return false
	}

	if clog.messageStore.config.MessageIndexEnable &&
		clog.messageStore.config.MessageIndexSafe {
		if storeTimestamp <= clog.messageStore.steCheckpoint.getMinTimestampIndex() {
			logger.Infof("find check timestamp, %d %s.", storeTimestamp,
				timeMillisecondToHumanString(time.Unix(storeTimestamp, 0)))
			return true
		}
	} else {
		if storeTimestamp <= clog.messageStore.steCheckpoint.getMinTimestamp() {
			logger.Infof("find check timestamp, %d %s.", storeTimestamp,
				timeMillisecondToHumanString(time.Unix(storeTimestamp, 0)))
			return true
		}
	}

	return false
}

func (clog *commitLog) deleteExpiredFile(expiredTime int64, deleteFilesInterval int32, intervalForcibly int64, cleanImmediately bool) int {
	return clog.mfq.deleteExpiredFileByTime(expiredTime, int(deleteFilesInterval), intervalForcibly, cleanImmediately)
}

func (clog *commitLog) retryDeleteFirstFile(intervalForcibly int64) bool {
	return clog.mfq.retryDeleteFirstFile(intervalForcibly)
}

func (clog *commitLog) getData(offset int64) *mappedBufferResult {
	returnFirstOnNotFound := false
	if offset == 0 {
		returnFirstOnNotFound = true
	}

	mf := clog.mfq.findMappedFileByOffset(offset, returnFirstOnNotFound)
	if mf != nil {
		mappedFileSize := clog.messageStore.config.MappedFileSizeCommitLog
		pos := offset % int64(mappedFileSize)
		result := mf.selectMappedBuffer(pos)
		return result
	}

	return nil
}

func (clog *commitLog) appendData(startOffset int64, data []byte) bool {
	clog.mutex.Lock()
	defer clog.mutex.Unlock()

	mf, err := clog.mfq.getLastMappedFile(startOffset)
	if err != nil {
		logger.Errorf("commit log append data get last mapped file err: %s.", err)
		return false
	}

	size := mf.byteBuffer.limit - mf.byteBuffer.writePos
	if len(data) > size {
		mf.appendMessage(data[:size])
		mf, err = clog.mfq.getLastMappedFile(startOffset + int64(size))
		if err != nil {
			logger.Errorf("commit log append data get last mapped file err: %s.", err)
			return false
		}

		return mf.appendMessage(data[size:])
	}

	return mf.appendMessage(data)
}

func (clog *commitLog) destroy() {
	if clog.mfq != nil {
		clog.mfq.destroy()
	}
}

func (clog *commitLog) start() {
	if clog.flushCLogService != nil {
		go clog.flushCLogService.start()
	}
}

func (clog *commitLog) shutdown() {
	if clog.flushCLogService != nil {
		clog.flushCLogService.shutdown()
	}
}

const (
	FlushRetryTimesOver = 3
)

// groupCommitRequest
// Author zhoufei
// Since 2017/10/18
type groupCommitRequest struct {
	nextOffset int64
	notify     *system.Notify
	flushOK    bool
}

func newGroupCommitRequest(nextOffset int64) *groupCommitRequest {
	request := new(groupCommitRequest)
	request.nextOffset = 0
	request.notify = system.CreateNotify()
	request.flushOK = false
	return request
}

func (gcreq *groupCommitRequest) wakeupCustomer(flushOK bool) {
	gcreq.flushOK = flushOK
	gcreq.notify.Signal()
}

func (gcreq *groupCommitRequest) waitForFlush(timeout int64) bool {
	gcreq.notify.WaitTimeout(time.Duration(timeout) * time.Millisecond)
	return gcreq.flushOK
}

type flushCommitLogService interface {
	start()
	shutdown()
}

// groupCommitService
// Author zhoufei
// Since 2017/10/18
type groupCommitService struct {
	requestsWrite *list.List
	requestsRead  *list.List
	putMutex      sync.Mutex
	swapMutex     sync.Mutex
	clog          *commitLog
	stoped        bool
	notify        *system.Notify
	hasNotified   bool
}

func newGroupCommitService(clog *commitLog) *groupCommitService {
	return &groupCommitService{
		requestsWrite: list.New(),
		requestsRead:  list.New(),
		clog:          clog,
		stoped:        false,
		notify:        system.CreateNotify(),
		hasNotified:   false,
	}
}

func (gcs *groupCommitService) putRequest(request *groupCommitRequest) {
	gcs.putMutex.Lock()
	defer gcs.putMutex.Unlock()

	gcs.requestsWrite.PushBack(request)
	if !gcs.hasNotified {
		gcs.hasNotified = true
		gcs.notify.Signal()
	}
}

func (gcs *groupCommitService) swapRequests() {
	tmp := gcs.requestsWrite
	gcs.requestsWrite = gcs.requestsRead
	gcs.requestsRead = tmp
}

func (gcs *groupCommitService) doCommit() {
	if gcs.requestsRead.Len() > 0 {
		for element := gcs.requestsRead.Front(); element != nil; element = element.Next() {
			request := element.Value.(*groupCommitRequest)
			flushOk := false
			for i := 0; i < 2 && !flushOk; i++ {
				flushOk = gcs.clog.mfq.committedWhere >= request.nextOffset
				if !flushOk {
					gcs.clog.mfq.commit(0)
				}
			}

			request.wakeupCustomer(flushOk)
		}

		storeTimestamp := gcs.clog.mfq.storeTimestamp
		if storeTimestamp > 0 {
			gcs.clog.messageStore.steCheckpoint.physicMsgTimestamp = storeTimestamp
		}

		gcs.requestsRead.Init()

	} else {
		gcs.clog.mfq.commit(0)
	}
}

func (gcs *groupCommitService) waitForRunning() {
	gcs.swapMutex.Lock()
	defer gcs.swapMutex.Unlock()

	if gcs.hasNotified {
		gcs.hasNotified = false
		gcs.swapRequests()
		return
	}

	gcs.notify.Wait()
	gcs.hasNotified = false
	gcs.swapRequests()
}

func (gcs *groupCommitService) start() {
	for {
		if gcs.stoped {
			break
		}

		gcs.waitForRunning()
		gcs.doCommit()
	}
}

func (gcs *groupCommitService) shutdown() {
	gcs.stoped = true

	// Under normal circumstances shutdown, wait for the arrival of the request, and then flush
	time.Sleep(10 * time.Millisecond)

	gcs.swapRequests()
	gcs.doCommit()

	logger.Info("group commit service end.")
}

type flushRealTimeService struct {
	lastFlushTimestamp int64
	printTimes         int64
	clog               *commitLog
	notify             *system.Notify
	hasNotified        bool
	stoped             bool
	mutex              sync.Mutex
}

func newFlushRealTimeService(clog *commitLog) *flushRealTimeService {
	fts := new(flushRealTimeService)
	fts.lastFlushTimestamp = 0
	fts.printTimes = 0
	fts.clog = clog
	fts.notify = system.CreateNotify()
	fts.hasNotified = false
	fts.stoped = false
	return fts
}

func (fts *flushRealTimeService) start() {
	logger.Info("flush real time service started.")

	for {
		if fts.stoped {
			break
		}

		var (
			flushCommitLogTimed              = fts.clog.messageStore.config.FlushCommitLogTimed
			interval                         = fts.clog.messageStore.config.FlushIntervalCommitLog
			flushPhysicQueueLeastPages       = fts.clog.messageStore.config.FlushCommitLogLeastPages
			flushPhysicQueueThoroughInterval = fts.clog.messageStore.config.FlushCommitLogThoroughInterval
			printFlushProgress               = false
			currentTimeMillis                = system.CurrentTimeMillis()
		)

		if currentTimeMillis >= fts.lastFlushTimestamp+int64(flushPhysicQueueThoroughInterval) {
			fts.lastFlushTimestamp = currentTimeMillis
			flushPhysicQueueLeastPages = 0
			fts.printTimes++
			printFlushProgress = fts.printTimes%10 == 0
		}

		if flushCommitLogTimed {
			time.Sleep(time.Duration(interval) * time.Millisecond)
		} else {
			fts.waitForRunning(int64(interval))
		}

		if printFlushProgress {
			fts.printFlushProgress()
		}

		fts.clog.mfq.commit(flushPhysicQueueLeastPages)
		storeTimestamp := fts.clog.mfq.storeTimestamp
		if storeTimestamp > 0 {
			fts.clog.messageStore.steCheckpoint.physicMsgTimestamp = storeTimestamp
		}
	}
}

func (fts *flushRealTimeService) printFlushProgress() {
	logger.Infof("how much disk fall behind memory, %d.", fts.clog.mfq.howMuchFallBehind())
}

func (fts *flushRealTimeService) waitForRunning(interval int64) {
	fts.mutex.Lock()
	defer fts.mutex.Unlock()

	if fts.hasNotified {
		fts.hasNotified = false
		return
	}

	fts.notify.WaitTimeout(time.Duration(interval) * time.Millisecond)
	fts.hasNotified = false
}

func (fts *flushRealTimeService) wakeup() {
	if !fts.hasNotified {
		fts.hasNotified = true
		fts.notify.Signal()
	}
}

func (fts *flushRealTimeService) destroy() {
	// Normal shutdown, to ensure that all the flush before exit
	result := false
	for i := 0; i < FlushRetryTimesOver && !result; i++ {
		result = fts.clog.mfq.commit(0)
		if result {
			logger.Infof("flush real time service shutdown, retry %d times success.", i+1)
		} else {
			logger.Infof("flush real time service shutdown, retry %d times failed.", i+1)
		}

	}

	fts.printFlushProgress()
	logger.Info("flush real time service end.")
}

func (fts *flushRealTimeService) shutdown() {
	fts.stoped = true
	fts.destroy()
}

const (
	END_FILE_MIN_BLANK_LENGTH   = 4 + 4
	TOTALSIZE                   = 4 // 1 TOTALSIZE
	MAGICCODE                   = 4 // 2 MAGICCODE
	BODYCRC                     = 4 // 3 BODYCRC
	QUEUE_ID                    = 4 // 4 QUEUEID
	FLAG                        = 4 // 5 FLAG
	QUEUE_OFFSET                = 8 // 6 QUEUEOFFSET
	PHYSICAL_OFFSET             = 8 // 7 PHYSICALOFFSET
	SYSFLAG                     = 4 // 8 SYSFLAG
	BORN_TIMESTAMP              = 8 // 9 BORNTIMESTAMP
	BORN_HOST                   = 8 // 10 BORNHOST
	STORE_TIMESTAMP             = 8 // 11 STORETIMESTAMP
	STORE_HOST_ADDRESS          = 8 // 12 STOREHOSTADDRESS
	RE_CONSUME_TIMES            = 4 // 13 RECONSUMETIMES
	PREPARED_TRANSACTION_OFFSET = 8 // 14 Prepared Transaction Offset
	BODY_LENGTH                 = 4
	TOPIC_LENGTH                = 1
	PROPERTIES_LENGTH           = 2
)

type defaultAppendMessageCallback struct {
	msgIdMemory        *mappedByteBuffer
	msgStoreItemMemory *mappedByteBuffer
	maxMessageSize     int32
	clog               *commitLog
}

func newDefaultAppendMessageCallback(size int32, clog *commitLog) *defaultAppendMessageCallback {
	cb := &defaultAppendMessageCallback{}
	cb.msgIdMemory = newMappedByteBuffer(make([]byte, message.MSG_ID_LENGTH))
	cb.msgStoreItemMemory = newMappedByteBuffer(make([]byte, size+END_FILE_MIN_BLANK_LENGTH))
	cb.maxMessageSize = size
	cb.clog = clog

	return cb
}

func (damcb *defaultAppendMessageCallback) doAppend(fileFromOffset int64, byteBuffer *mappedByteBuffer, maxBlank int32, msg interface{}) *store.AppendMessageResult {
	// TODO
	msgInner, ok := msg.(*store.MessageExtInner)
	if !ok {
		// TODO
	}

	wroteOffset := fileFromOffset + int64(byteBuffer.writePos)
	msgId, err := message.CreateMessageId(msgInner.StoreHost, wroteOffset)
	if err != nil {
		// TODO
	}

	key := msgInner.Topic + "-" + strconv.Itoa(int(msgInner.QueueId))
	queryOffset, ok := damcb.clog.topicQueueTable[key]
	if !ok {
		queryOffset = int64(0)
		damcb.clog.topicQueueTable[key] = queryOffset
	}

	// TODO Transaction messages that require special handling

	// Serialize message
	propertiesData := []byte(msgInner.PropertiesString)
	propertiesContentLength := len(propertiesData)
	topicData := []byte(msgInner.Topic)
	topicContentLength := len(topicData)
	bodyContentLength := len(msgInner.Body)

	msgLen := int32(TOTALSIZE + MAGICCODE + BODYCRC + QUEUE_ID + FLAG + QUEUE_OFFSET + PHYSICAL_OFFSET +
		SYSFLAG + BORN_TIMESTAMP + BORN_HOST + STORE_TIMESTAMP + STORE_HOST_ADDRESS + RE_CONSUME_TIMES +
		PREPARED_TRANSACTION_OFFSET + BODY_LENGTH + bodyContentLength + TOPIC_LENGTH + topicContentLength +
		PROPERTIES_LENGTH + propertiesContentLength)

	// Exceeds the maximum message
	if msgLen > damcb.maxMessageSize {
		logger.Errorf("message size exceeded, msg total size: %d, msg body size: %d, maxMessageSize: %d.",
			msgLen, bodyContentLength, damcb.maxMessageSize)

		return &store.AppendMessageResult{Status: store.MESSAGE_SIZE_EXCEEDED}
	}

	// Determines whether there is sufficient free space
	spaceLen := msgLen + int32(END_FILE_MIN_BLANK_LENGTH)
	if spaceLen > maxBlank {
		damcb.resetMsgStoreItemMemory(maxBlank)
		damcb.msgStoreItemMemory.WriteInt32(maxBlank)
		blankMagicCode := BlankMagicCode
		damcb.msgStoreItemMemory.WriteInt32(int32(blankMagicCode))
		damcb.msgStoreItemMemory.Write(make([]byte, maxBlank-8))

		data := damcb.msgStoreItemMemory.Bytes()
		byteBuffer.Write(data)

		return &store.AppendMessageResult{
			Status:         store.END_OF_FILE,
			WroteOffset:    wroteOffset,
			WroteBytes:     int64(maxBlank),
			MsgId:          msgId,
			StoreTimestamp: msgInner.StoreTimestamp,
			LogicsOffset:   queryOffset}
	}

	messageMagicCode := MessageMagicCode

	// Initialization of storage space
	damcb.resetMsgStoreItemMemory(msgLen)
	damcb.msgStoreItemMemory.WriteInt32(msgLen)                                         // 1 TOTALSIZE
	damcb.msgStoreItemMemory.WriteInt32(int32(messageMagicCode))                        // 2 MAGICCODE
	damcb.msgStoreItemMemory.WriteInt32(msgInner.BodyCRC)                               // 3 BODYCRC
	damcb.msgStoreItemMemory.WriteInt32(msgInner.QueueId)                               // 4 QUEUEID
	damcb.msgStoreItemMemory.WriteInt32(msgInner.Flag)                                  // 5 FLAG
	damcb.msgStoreItemMemory.WriteInt64(queryOffset)                                    // 6 QUEUEOFFSET
	damcb.msgStoreItemMemory.WriteInt64(fileFromOffset + int64(byteBuffer.writePos))    // 7 PHYSICALOFFSET
	damcb.msgStoreItemMemory.WriteInt32(msgInner.SysFlag)                               // 8 SYSFLAG
	damcb.msgStoreItemMemory.WriteInt64(msgInner.BornTimestamp)                         // 9 BORNTIMESTAMP
	damcb.msgStoreItemMemory.Write(damcb.hostStringToBytes(msgInner.BornHost))          // 10 BORNHOST
	damcb.msgStoreItemMemory.WriteInt64(msgInner.StoreTimestamp)                        // 11 STORETIMESTAMP
	damcb.msgStoreItemMemory.Write([]byte(damcb.hostStringToBytes(msgInner.StoreHost))) // 12 STOREHOSTADDRESS
	damcb.msgStoreItemMemory.WriteInt32(msgInner.ReconsumeTimes)                        // 13 RECONSUMETIMES
	damcb.msgStoreItemMemory.WriteInt64(msgInner.PreparedTransactionOffset)             // 14 Prepared Transaction Offset
	damcb.msgStoreItemMemory.WriteInt32(int32(bodyContentLength))                       // 15 BODY
	if bodyContentLength > 0 {
		damcb.msgStoreItemMemory.Write(msgInner.Body) // BODY Content
	}

	damcb.msgStoreItemMemory.WriteInt8(int8(topicContentLength)) // 16 TOPIC
	damcb.msgStoreItemMemory.Write(topicData)

	damcb.msgStoreItemMemory.WriteInt16(int16(propertiesContentLength)) // 17 PROPERTIES
	if propertiesContentLength > 0 {
		damcb.msgStoreItemMemory.Write(propertiesData)
	}

	byteBuffer.Write(damcb.msgStoreItemMemory.Bytes())

	result := &store.AppendMessageResult{
		Status:         store.APPENDMESSAGE_PUT_OK,
		WroteOffset:    wroteOffset,
		WroteBytes:     int64(msgLen),
		MsgId:          msgId,
		StoreTimestamp: msgInner.StoreTimestamp,
		LogicsOffset:   queryOffset}

	tranType := sysflag.GetTransactionValue(int(msgInner.SysFlag))
	switch tranType {
	case sysflag.TransactionPreparedType:
		// TODO 事务消息处理
		break
	case sysflag.TransactionRollbackType:
		// TODO 事务消息处理
		break
	case sysflag.TransactionNotType:
		fallthrough
	case sysflag.TransactionCommitType:
		atomic.AddInt64(&queryOffset, 1) // The next update ConsumeQueue information
		damcb.clog.topicQueueTable[key] = atomic.LoadInt64(&queryOffset)
		break
	default:
		break
	}

	return result
}

func (damcb *defaultAppendMessageCallback) hostStringToBytes(hostAddr string) []byte {
	host, port, err := message.SplitHostPort(hostAddr)
	if err != nil {
		logger.Warnf("parse message %s err: %s.", hostAddr, err)
		return make([]byte, 8)
	}

	ip := net.ParseIP(host)
	ipBytes := []byte(ip)

	var addrBytes *bytes.Buffer
	if len(ipBytes) > 0 {
		addrBytes = bytes.NewBuffer(ipBytes[12:])
	} else {
		addrBytes = bytes.NewBuffer(make([]byte, 4))
	}

	binary.Write(addrBytes, binary.BigEndian, &port)
	return addrBytes.Bytes()
}

func (damcb *defaultAppendMessageCallback) resetMsgStoreItemMemory(length int32) {
	// TODO 初始化数据
	damcb.msgStoreItemMemory.limit = damcb.msgStoreItemMemory.writePos
	damcb.msgStoreItemMemory.writePos = 0
	damcb.msgStoreItemMemory.limit = int(length)
	if damcb.msgStoreItemMemory.writePos > damcb.msgStoreItemMemory.limit {
		damcb.msgStoreItemMemory.writePos = damcb.msgStoreItemMemory.limit
	}
}

const (
	MaxManualDeleteFileTimes = 20 // 手工触发一次最多删除次数
)

// cleanCommitLogService 清理物理文件服务
// Author zhoufei
// Since 2017/10/13
type cleanCommitLogService struct {
	diskSpaceWarningLevelRatio   float64 // 磁盘空间警戒水位，超过，则停止接收新消息（出于保护自身目的）
	diskSpaceCleanForciblyRatio  float64 // 磁盘空间强制删除文件水位
	lastRedeleteTimestamp        int64   // 最后清理时间
	manualDeleteFileSeveralTimes int64   // 手工触发删除消息
	cleanImmediately             bool    // 立刻开始强制删除文件
	messageStore                 *PersistentMessageStore
}

func newCleanCommitLogService(messageStore *PersistentMessageStore) *cleanCommitLogService {
	ccls := new(cleanCommitLogService)
	ccls.diskSpaceWarningLevelRatio = ccls.parseFloatProperty(
		"boltmq.broker.diskSpaceWarningLevelRatio", 0.90)
	ccls.diskSpaceCleanForciblyRatio = ccls.parseFloatProperty(
		"boltmq.broker.diskSpaceCleanForciblyRatio", 0.85)
	ccls.lastRedeleteTimestamp = 0
	ccls.manualDeleteFileSeveralTimes = 0
	ccls.cleanImmediately = false
	ccls.messageStore = messageStore

	return ccls
}

func (ccls *cleanCommitLogService) parseFloatProperty(propertyName string, defaultValue float64) float64 {
	value := ccls.getSystemProperty(propertyName)
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultValue
	}

	result, err := strconv.ParseFloat(value, 64)
	if err != nil {
		logger.Warnf("parse property(%s) set default value %f, err: %s.", propertyName, defaultValue, err)
		result = defaultValue
	}

	return result
}

func (ccls *cleanCommitLogService) getSystemProperty(name string) string {
	value := os.Getenv(name)
	return value
}

func (ccls *cleanCommitLogService) run() {
	ccls.deleteExpiredFiles()
	ccls.redeleteHangedFile()
}

func (ccls *cleanCommitLogService) deleteExpiredFiles() {
	timeup := ccls.isTimeToDelete()
	spacefull := ccls.isSpaceToDelete()
	manualDelete := ccls.manualDeleteFileSeveralTimes > 0

	// 删除物理队列文件
	if timeup || spacefull || manualDelete {
		if manualDelete {
			ccls.manualDeleteFileSeveralTimes--
		}

		fileReservedTime := ccls.messageStore.config.FileReservedTime

		// 是否立刻强制删除文件
		cleanAtOnce := ccls.messageStore.config.CleanFileForciblyEnable && ccls.cleanImmediately
		logger.Infof("begin to delete before %d hours file. timeup: %t spacefull: %t manualDeleteFileSeveralTimes: %d cleanAtOnce: %t.",
			fileReservedTime, timeup, spacefull, ccls.manualDeleteFileSeveralTimes, cleanAtOnce)

		// 小时转化成毫秒
		fileReservedTime *= 60 * 60 * 1000

		deletePhysicFilesInterval := ccls.messageStore.config.DeleteCommitLogFilesInterval
		destroyMappedFileIntervalForcibly := ccls.messageStore.config.DestroyMappedFileIntervalForcibly

		deleteCount := ccls.messageStore.clog.deleteExpiredFile(fileReservedTime,
			deletePhysicFilesInterval, int64(destroyMappedFileIntervalForcibly), cleanAtOnce)

		if deleteCount > 0 {
			// TODO
		} else if spacefull { // 危险情况：磁盘满了，但是又无法删除文件
			logger.Warn("disk space will be full soon, but delete file failed.")
		}
	}
}

func (ccls *cleanCommitLogService) isTimeToDelete() bool {
	when := ccls.messageStore.config.DeleteWhen
	if isItTimeToDo(when) {
		logger.Infof("it's time to reclaim disk space, %s.", when)
		return true
	}

	return false
}

func (ccls *cleanCommitLogService) isSpaceToDelete() bool {
	ccls.cleanImmediately = false

	// 检测物理文件磁盘空间
	if ccls.checkCommitLogFileSpace() {
		return true
	}

	// 检测逻辑文件磁盘空间
	if ccls.checkConsumeQueueFileSpace() {
		return true
	}

	return false
}

func (ccls *cleanCommitLogService) checkCommitLogFileSpace() bool {
	var (
		ratio           = float64(ccls.messageStore.config.getDiskMaxUsedSpaceRatio()) / 100.0
		storePathPhysic = ccls.messageStore.config.StorePathCommitLog
		physicRatio     = common.GetDiskPartitionSpaceUsedPercent(storePathPhysic)
	)

	if physicRatio > ccls.diskSpaceWarningLevelRatio {
		diskFull := ccls.messageStore.runFlags.getAndMakeDiskFull()
		if diskFull {
			logger.Errorf("physic disk maybe full soon %f, so mark disk full.", physicRatio)
			// TODO System.gc()
		}

		ccls.cleanImmediately = true
	} else if physicRatio > ccls.diskSpaceCleanForciblyRatio {
		ccls.cleanImmediately = true
	} else {
		diskOK := ccls.messageStore.runFlags.getAndMakeDiskOK()
		if !diskOK {
			logger.Infof("physic disk space OK %f, so mark disk ok.", physicRatio)
		}
	}

	if physicRatio < 0 || physicRatio > ratio {
		logger.Infof("physic disk maybe full soon, so reclaim space, %f.", physicRatio)
		return true
	}

	return false
}

func (ccls *cleanCommitLogService) checkConsumeQueueFileSpace() bool {
	var (
		ratio           = float64(ccls.messageStore.config.getDiskMaxUsedSpaceRatio()) / 100.0
		storePathLogics = common.GetStorePathConsumeQueue(ccls.messageStore.config.StorePathRootDir)
		logicsRatio     = common.GetDiskPartitionSpaceUsedPercent(storePathLogics)
	)

	if logicsRatio > ccls.diskSpaceWarningLevelRatio {
		diskFull := ccls.messageStore.runFlags.getAndMakeDiskFull()
		if diskFull {
			logger.Errorf("logics disk maybe full soon %f, so mark disk full.", logicsRatio)
			// TODO System.gc()
		}

		ccls.cleanImmediately = true
	} else if logicsRatio > ccls.diskSpaceCleanForciblyRatio {
		ccls.cleanImmediately = true
	} else {
		diskOK := ccls.messageStore.runFlags.getAndMakeDiskOK()
		if !diskOK {
			logger.Infof("logics disk space OK %f, so mark disk ok.", logicsRatio)
		}
	}

	if logicsRatio < 0 || logicsRatio > ratio {
		logger.Infof("logics disk maybe full soon, so reclaim space, %f.", logicsRatio)
		return true
	}

	return false
}

func (ccls *cleanCommitLogService) redeleteHangedFile() {
	interval := ccls.messageStore.config.RedeleteHangedFileInterval
	currentTimestamp := system.CurrentTimeMillis()
	if (currentTimestamp - ccls.lastRedeleteTimestamp) > int64(interval) {
		ccls.lastRedeleteTimestamp = currentTimestamp
		destroyMappedFileIntervalForcibly := ccls.messageStore.config.DestroyMappedFileIntervalForcibly
		ccls.messageStore.clog.retryDeleteFirstFile(int64(destroyMappedFileIntervalForcibly))
	}
}

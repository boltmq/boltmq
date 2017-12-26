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
package store

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltmq/boltmq/stgcommon"
	"github.com/boltmq/boltmq/store/core"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
)

const (
	MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8
	BlankMagicCode   = 0xBBCCDDEE ^ 1880681586 + 8
)

type DispatchRequest struct {
	topic                     string
	queueId                   int32
	commitLogOffset           int64
	msgSize                   int64
	tagsCode                  int64
	storeTimestamp            int64
	consumeQueueOffset        int64
	keys                      string
	sysFlag                   int32
	preparedTransactionOffset int64
	producerGroup             string
	tranStateTableOffset      int64
}

type CommitLog struct {
	mapedFileQueue        *core.MapedFileQueue
	defaultMessageStore   *DefaultMessageStore
	flushCommitLogService FlushCommitLogService
	appendMessageCallback core.AppendMessageCallback
	topicQueueTable       map[string]int64
	mutex                 sync.Mutex
}

func newCommitLog(defaultMessageStore *DefaultMessageStore) *CommitLog {
	commitLog := &CommitLog{}
	commitLog.mapedFileQueue = core.NewMapedFileQueue(defaultMessageStore.config.StorePathCommitLog,
		int64(defaultMessageStore.config.MapedFileSizeCommitLog), defaultMessageStore.allocateMapedFileService)
	commitLog.defaultMessageStore = defaultMessageStore

	if SYNC_FLUSH == defaultMessageStore.config.FlushDisk {
		commitLog.flushCommitLogService = newGroupCommitService(commitLog)
	} else {
		commitLog.flushCommitLogService = newFlushRealTimeService(commitLog)
	}

	commitLog.topicQueueTable = make(map[string]int64, 1024)
	commitLog.appendMessageCallback = newDefaultAppendMessageCallback(defaultMessageStore.config.MaxMessageSize, commitLog)
	return commitLog
}

func (clog *CommitLog) Load() bool {
	result := clog.mapedFileQueue.Load()

	if result {
		logger.Info("load commit log OK")
	} else {
		logger.Info("load commit log Failed")
	}

	return result
}

func (clog *CommitLog) putMessage(msg *MessageExtBrokerInner) *core.PutMessageResult {
	msg.StoreTimestamp = time.Now().UnixNano() / 1000000
	msg.BodyCRC, _ = stgcommon.Crc32(msg.Body)

	// TODO 事务消息处理
	clog.mutex.Lock()
	beginLockTimestamp := time.Now().UnixNano() / 1000000
	msg.BornTimestamp = beginLockTimestamp

	mapedFile, err := clog.mapedFileQueue.GetLastMapedFile(int64(0))
	if err != nil {
		// TODO
		return &core.PutMessageResult{Status: core.CREATE_MAPEDFILE_FAILED}
	}

	if mapedFile == nil {
		// TODO
		return &core.PutMessageResult{Status: core.CREATE_MAPEDFILE_FAILED}
	}

	result := mapedFile.AppendMessageWithCallBack(msg, clog.appendMessageCallback)
	switch result.Status {
	case core.APPENDMESSAGE_PUT_OK:
		break
	case core.END_OF_FILE:
		mapedFile, err = clog.mapedFileQueue.GetLastMapedFile(int64(0))
		if err != nil {
			logger.Error(err.Error())
			return &core.PutMessageResult{Status: core.CREATE_MAPEDFILE_FAILED, Result: result}
		}

		if mapedFile == nil {
			logger.Errorf("create maped file2 error, topic:%s clientAddr:%s", msg.Topic, msg.BornHost)
			return &core.PutMessageResult{Status: core.CREATE_MAPEDFILE_FAILED, Result: result}
		}

		result = mapedFile.AppendMessageWithCallBack(msg, clog.appendMessageCallback)
		break
	case core.MESSAGE_SIZE_EXCEEDED:
		return &core.PutMessageResult{Status: core.MESSAGE_ILLEGAL, Result: result}
	default:
		return &core.PutMessageResult{Status: core.PUTMESSAGE_UNKNOWN_ERROR, Result: result}
	}

	// DispatchRequest
	dispatchRequest := &DispatchRequest{
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

	clog.defaultMessageStore.DispatchMessageService.putRequest(dispatchRequest)

	eclipseTimeInLock := time.Now().UnixNano()/1000000 - beginLockTimestamp
	clog.mutex.Unlock()

	if eclipseTimeInLock > 1000 {
		logger.Warn("putMessage in lock eclipse time(ms) ", eclipseTimeInLock)
	}

	putMessageResult := &core.PutMessageResult{Status: core.PUTMESSAGE_PUT_OK, Result: result}

	// Statistics
	size := clog.defaultMessageStore.StoreStatsService.getSinglePutMessageTopicSizeTotal(msg.Topic)
	clog.defaultMessageStore.StoreStatsService.setSinglePutMessageTopicSizeTotal(msg.Topic, atomic.AddInt64(&size, result.WroteBytes))

	// Synchronization flush
	if SYNC_FLUSH == clog.defaultMessageStore.config.FlushDisk {
		if msg.isWaitStoreMsgOK() {
			request := NewGroupCommitRequest(result.WroteOffset + result.WroteBytes)

			if groupCommitService, ok := clog.flushCommitLogService.(*GroupCommitService); ok {
				groupCommitService.putRequest(request)

				flushOk := request.waitForFlush(int64(clog.defaultMessageStore.config.SyncFlushTimeout))
				if flushOk == false {
					logger.Errorf("do groupcommit, wait for flush failed, topic: %s tags: %s client address: %s",
						msg.Topic, msg.GetTags(), msg.BornHost)
					putMessageResult.Status = core.FLUSH_DISK_TIMEOUT
				}
			}
		}
	} else {
		if flushRealTimeService, ok := clog.flushCommitLogService.(*FlushRealTimeService); ok {
			flushRealTimeService.wakeup()
		}
	}

	// Synchronous write double
	if SYNC_MASTER == clog.defaultMessageStore.config.BrokerRole {
		// TODO
	}

	return putMessageResult
}

/*
func (clog *CommitLog) getMessage(offset int64, size int32) *SelectMapedBufferResult {
	returnFirstOnNotFound := false
	if 0 == offset {
		returnFirstOnNotFound = true
	}

	mapedFile := clog.mapedFileQueue.findMapedFileByOffset(offset, returnFirstOnNotFound)
	if mapedFile != nil {
		mapedFileSize := clog.defaultMessageStore.config.MapedFileSizeCommitLog
		pos := offset % int64(mapedFileSize)
		result := mapedFile.selectMapedBufferByPosAndSize(pos, size)
		return result
	}

	return nil
}

func (clog *CommitLog) rollNextFile(offset int64) int64 {
	mapedFileSize := clog.defaultMessageStore.config.MapedFileSizeCommitLog
	nextOffset := offset + int64(mapedFileSize) - offset%int64(mapedFileSize)
	return nextOffset
}

func (clog *CommitLog) recoverNormally() {
	checkCRCOnRecover := clog.defaultMessageStore.config.CheckCRCOnRecover
	mapedFiles := clog.mapedFileQueue.mapedFiles
	if mapedFiles != nil && mapedFiles.Len() > 0 {
		index := mapedFiles.Len() - 3
		if index < 0 {
			index = 0
		}

		mapedFile := getMapedFileByIndex(mapedFiles, index)
		mappedByteBuffer := NewMappedByteBuffer(mapedFile.mappedByteBuffer.Bytes())
		mappedByteBuffer.WritePos = mapedFile.mappedByteBuffer.WritePos
		processOffset := mapedFile.fileFromOffset
		mapedFileOffset := int64(0)
		for {
			dispatchRequest := clog.checkMessageAndReturnSize(mappedByteBuffer,
				clog.defaultMessageStore.config.CheckCRCOnRecover, checkCRCOnRecover)
			size := dispatchRequest.msgSize
			if size > 0 {
				mapedFileOffset += size
			} else if size == -1 {
				logger.Info("recover physics file end, ", mapedFile.fileName)
				break
			} else if size == 0 {
				index++
				if index >= mapedFiles.Len() {
					logger.Info("recover last 3 physics file over, last maped file ", mapedFile.fileName)
					break
				} else {
					mapedFile = getMapedFileByIndex(mapedFiles, index)
					mappedByteBuffer = mapedFile.mappedByteBuffer
					mappedByteBuffer.WritePos = mapedFile.mappedByteBuffer.WritePos
					processOffset = mapedFile.fileFromOffset
					mapedFileOffset = 0
				}
			}
		}

		processOffset += mapedFileOffset
		clog.mapedFileQueue.committedWhere = processOffset
		clog.mapedFileQueue.truncateDirtyFiles(processOffset)

	}
}

func (clog *CommitLog) pickupStoretimestamp(offset int64, size int32) int64 {
	if offset > clog.getMinOffset() {
		result := clog.getMessage(offset, size)
		if result != nil {
			defer result.Release()
			result.MappedByteBuffer.ReadPos = message.MessageStoreTimestampPostion
			storeTimestamp := result.MappedByteBuffer.ReadInt64()
			return storeTimestamp
		}
	}

	return -1
}

func (clog *CommitLog) getMinOffset() int64 {
	mapedFile := clog.mapedFileQueue.getFirstMapedFileOnLock()
	if mapedFile != nil {
		// TODO mapedFile.isAvailable()
		return mapedFile.fileFromOffset
	}

	return -1
}

func (clog *CommitLog) getMaxOffset() int64 {
	return clog.mapedFileQueue.getMaxOffset()
}

func (clog *CommitLog) removeQueurFromtopicQueueTable(topic string, queueId int32) {
	clog.mutex.Lock()
	clog.mutex.Unlock()
	key := fmt.Sprintf("%s-%d", topic, queueId)
	delete(clog.topicQueueTable, key)
}

func (clog *CommitLog) checkMessageAndReturnSize(mappedByteBuffer *MappedByteBuffer, checkCRC bool, readBody bool) *DispatchRequest {
	messageMagicCode := MessageMagicCode
	blankMagicCode := BlankMagicCode

	bufferLen := mappedByteBuffer.WritePos - mappedByteBuffer.ReadPos
	totalSize := mappedByteBuffer.ReadInt32() // 1 TOTALSIZE
	magicCode := mappedByteBuffer.ReadInt32() // 2 MAGICCODE

	switch magicCode {
	case int32(messageMagicCode):
		break
	case int32(blankMagicCode):
		return &DispatchRequest{msgSize: 0}
	default:
		logger.Warnf("found a illegal magic code MessageMagicCode:%d BlankMagicCode:%d ActualMagicCode:%d",
			messageMagicCode, blankMagicCode, magicCode)
		return &DispatchRequest{msgSize: -1}
	}

	if totalSize > int32(bufferLen) {
		return &DispatchRequest{msgSize: -1}
	}

	bodyCRC := mappedByteBuffer.ReadInt32()                   // 3 BODYCRC
	queueId := mappedByteBuffer.ReadInt32()                   // 4 QUEUEID
	mappedByteBuffer.ReadInt32()                              // 5 FLAG
	queueOffset := mappedByteBuffer.ReadInt64()               // 6 QUEUEOFFSET
	physicOffset := mappedByteBuffer.ReadInt64()              // 7 PHYSICALOFFSET
	sysFlag := mappedByteBuffer.ReadInt32()                   // 8 SYSFLAG
	mappedByteBuffer.ReadInt64()                              // 9 BORNTIMESTAMP
	mappedByteBuffer.Read(make([]byte, 8))                    // 10 BORNHOST（IP+PORT）
	storeTimestamp := mappedByteBuffer.ReadInt64()            // 11 STORETIMESTAMP
	mappedByteBuffer.Read(make([]byte, 8))                    // 12 STOREHOST（IP+PORT）
	mappedByteBuffer.ReadInt32()                              // 13 RECONSUMETIMES
	preparedTransactionOffset := mappedByteBuffer.ReadInt64() // 14 Prepared Transaction Offset
	// 15 BODY
	bodyLen := mappedByteBuffer.ReadInt32()
	if bodyLen > 0 {
		if readBody {
			bodyContent := make([]byte, bodyLen)
			mappedByteBuffer.Read(bodyContent)
			if checkCRC {
				crc, _ := stgcommon.Crc32(bodyContent)
				if int32(crc) != bodyCRC {
					logger.Warnf("CRC check failed crc:%d, bodyCRC:%d", crc, bodyCRC)
					return &DispatchRequest{msgSize: -1}
				}
			}
		} else {
			mappedByteBuffer.ReadPos = mappedByteBuffer.ReadPos + int(bodyLen)
		}
	}

	var (
		topic          = ""
		keys           = ""
		tagsCode int64 = 0
	)

	// 16 TOPIC
	topicLen := mappedByteBuffer.ReadInt8()
	if topicLen > 0 {
		topicBytes := make([]byte, topicLen)
		mappedByteBuffer.Read(topicBytes)
		topic = string(topicBytes)
	}

	// 17 properties
	propertiesLength := mappedByteBuffer.ReadInt16()
	if propertiesLength > 0 {
		propertiesBytes := make([]byte, propertiesLength)
		mappedByteBuffer.Read(propertiesBytes)
		properties := string(propertiesBytes)
		propertiesMap := message.String2messageProperties(properties)
		keys = propertiesMap[message.PROPERTY_KEYS]
		tags := propertiesMap[message.PROPERTY_TAGS]
		if len(tags) > 0 {
			tagsCode = TagsString2tagsCode(message.ParseTopicFilterType(sysFlag), tags)
		}

		// Timing message processing
		delayLevelStr, ok := propertiesMap[message.PROPERTY_DELAY_TIME_LEVEL]
		if SCHEDULE_TOPIC == topic && ok {
			delayLevelTemp, _ := strconv.Atoi(delayLevelStr)
			delayLevel := int32(delayLevelTemp)

			if delayLevel > clog.defaultMessageStore.ScheduleMessageService.maxDelayLevel {
				delayLevel = clog.defaultMessageStore.ScheduleMessageService.maxDelayLevel
			}

			if delayLevel > 0 {
				tagsCode = clog.defaultMessageStore.ScheduleMessageService.computeDeliverTimestamp(delayLevel, storeTimestamp)
			}
		}
	}

	return &DispatchRequest{
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

func (clog *CommitLog) recoverAbnormally() {
	checkCRCOnRecover := clog.defaultMessageStore.config.CheckCRCOnRecover
	mapedFiles := clog.mapedFileQueue.mapedFiles
	if mapedFiles.Len() > 0 {
		// Looking beginning to recover from which file
		var mapedFile *MapedFile
		index := mapedFiles.Len() - 1

		for element := mapedFiles.Back(); element != nil; element = element.Prev() {
			index--
			mapedFile = element.Value.(*MapedFile)
			if clog.isMapedFileMatchedRecover(mapedFile) {
				logger.Info("recover from this maped file ", mapedFile.fileName)
				break
			}
		}

		if mapedFile != nil {
			mappedByteBuffer := NewMappedByteBuffer(mapedFile.mappedByteBuffer.Bytes())
			mappedByteBuffer.WritePos = mapedFile.mappedByteBuffer.WritePos
			processOffset := mapedFile.fileFromOffset
			mapedFileOffset := int64(0)

			for {
				dispatchRequest := clog.checkMessageAndReturnSize(mappedByteBuffer, checkCRCOnRecover, true)
				size := dispatchRequest.msgSize

				if size > 0 { // Normal data
					mapedFileOffset += size
					clog.defaultMessageStore.putDispatchRequest(dispatchRequest)
				} else if size == -1 { // Intermediate file read error
					logger.Info("recover physics file end, ", mapedFile.fileName)
					break
				} else if size == 0 {
					index++

					if index >= mapedFiles.Len() { // The current branch under normal circumstances should
						logger.Info("recover physics file over, last maped file ", mapedFile.fileName)
						break
					} else {
						i := 0
						for element := mapedFiles.Front(); element != nil; element = element.Next() {
							if i == index {
								mapedFile = element.Value.(*MapedFile)
								mappedByteBuffer = NewMappedByteBuffer(mapedFile.mappedByteBuffer.Bytes())
								mappedByteBuffer.WritePos = mapedFile.mappedByteBuffer.WritePos
								processOffset = mapedFile.fileFromOffset
								mapedFileOffset = 0
								logger.Info("recover next physics file,", mapedFile.fileName)
								break
							}

							i++
						}
					}
				}
			}

			processOffset += mapedFileOffset
			clog.mapedFileQueue.committedWhere = processOffset
			clog.mapedFileQueue.truncateDirtyFiles(processOffset)

			// Clear ConsumeQueue redundant data
			clog.defaultMessageStore.truncateDirtyLogicFiles(processOffset)
		} else {
			// Commitlog case files are deleted
			clog.mapedFileQueue.committedWhere = 0
			clog.defaultMessageStore.destroyLogics()
		}
	}
}

func (clog *CommitLog) isMapedFileMatchedRecover(mapedFile *MapedFile) bool {
	byteBuffer := mapedFile.mappedByteBuffer.Bytes()
	if len(byteBuffer) == 0 {
		return false
	}

	mappedByteBuffer := NewMappedByteBuffer(byteBuffer)
	mappedByteBuffer.WritePos = mapedFile.mappedByteBuffer.WritePos
	mappedByteBuffer.ReadPos = message.MessageMagicCodePostion
	magicCode := mappedByteBuffer.ReadInt32()
	messageMagicCode := MessageMagicCode
	if magicCode != int32(messageMagicCode) {
		return false
	}

	mappedByteBuffer.ReadPos = message.MessageStoreTimestampPostion
	storeTimestamp := mappedByteBuffer.ReadInt64()
	if 0 == storeTimestamp {
		return false
	}

	if clog.defaultMessageStore.config.MessageIndexEnable &&
		clog.defaultMessageStore.config.MessageIndexSafe {
		if storeTimestamp <= clog.defaultMessageStore.StoreCheckpoint.getMinTimestampIndex() {
			logger.Infof("find check timestamp, %d %s", storeTimestamp,
				utils.TimeMillisecondToHumanString(time.Unix(storeTimestamp, 0)))
			return true
		}
	} else {
		if storeTimestamp <= clog.defaultMessageStore.StoreCheckpoint.getMinTimestamp() {
			logger.Infof("find check timestamp, %d %s", storeTimestamp,
				utils.TimeMillisecondToHumanString(time.Unix(storeTimestamp, 0)))
			return true
		}
	}

	return false
}

func (clog *CommitLog) deleteExpiredFile(expiredTime int64, deleteFilesInterval int32, intervalForcibly int64, cleanImmediately bool) int {
	return clog.mapedFileQueue.deleteExpiredFileByTime(expiredTime, int(deleteFilesInterval), intervalForcibly, cleanImmediately)
}

func (clog *CommitLog) retryDeleteFirstFile(intervalForcibly int64) bool {
	return clog.mapedFileQueue.retryDeleteFirstFile(intervalForcibly)
}

func (clog *CommitLog) getData(offset int64) *SelectMapedBufferResult {
	returnFirstOnNotFound := false
	if offset == 0 {
		returnFirstOnNotFound = true
	}

	mapedFile := clog.mapedFileQueue.findMapedFileByOffset(offset, returnFirstOnNotFound)
	if mapedFile != nil {
		mapedFileSize := clog.defaultMessageStore.config.MapedFileSizeCommitLog
		pos := offset % int64(mapedFileSize)
		result := mapedFile.selectMapedBuffer(pos)
		return result
	}

	return nil
}

func (clog *CommitLog) appendData(startOffset int64, data []byte) bool {
	clog.mutex.Lock()
	defer clog.mutex.Unlock()

	mapedFile, err := clog.mapedFileQueue.getLastMapedFile(startOffset)
	if err != nil {
		logger.Error("commit log append data get last maped file error:", err.Error())
		return false
	}

	size := mapedFile.mappedByteBuffer.Limit - mapedFile.mappedByteBuffer.WritePos
	if len(data) > size {
		mapedFile.appendMessage(data[:size])
		mapedFile, err = clog.mapedFileQueue.getLastMapedFile(startOffset + int64(size))
		if err != nil {
			logger.Error("commit log append data get last maped file error:", err.Error())
			return false
		}

		return mapedFile.appendMessage(data[size:])
	}

	return mapedFile.appendMessage(data)
}

func (clog *CommitLog) destroy() {
	if clog.mapedFileQueue != nil {
		clog.mapedFileQueue.destroy()
	}
}

func (clog *CommitLog) Start() {
	if SYNC_FLUSH == clog.defaultMessageStore.config.FlushDisk {
		if clog.GroupCommitService != nil {
			go clog.GroupCommitService.start()
		}
	} else {
		if clog.FlushRealTimeService != nil {
			go clog.FlushRealTimeService.start()
		}
	}
}

func (clog *CommitLog) Shutdown() {
	if SYNC_FLUSH == clog.defaultMessageStore.config.FlushDisk {
		if clog.GroupCommitService != nil {
			clog.GroupCommitService.shutdown()
		}
	} else {
		if clog.FlushRealTimeService != nil {
			clog.FlushRealTimeService.shutdown()
		}
	}
}
*/

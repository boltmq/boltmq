package stgstorelog

import (
	"sync"
	"time"

	"fmt"
	"strconv"
	"sync/atomic"

	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
)

const (
	MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8
	BlankMagicCode   = 0xBBCCDDEE ^ 1880681586 + 8
)

type CommitLog struct {
	MapedFileQueue        *MapedFileQueue
	DefaultMessageStore   *DefaultMessageStore
	FlushCommitLogService FlushCommitLogService
	AppendMessageCallback *DefaultAppendMessageCallback
	TopicQueueTable       map[string]int64
	mutex                 *sync.Mutex
}

func NewCommitLog(defaultMessageStore *DefaultMessageStore) *CommitLog {
	commitLog := &CommitLog{}
	commitLog.MapedFileQueue = NewMapedFileQueue(defaultMessageStore.MessageStoreConfig.StorePathCommitLog,
		int64(defaultMessageStore.MessageStoreConfig.MapedFileSizeCommitLog),
		defaultMessageStore.AllocateMapedFileService)
	commitLog.DefaultMessageStore = defaultMessageStore
	commitLog.mutex = new(sync.Mutex)

	if config.SYNC_FLUSH == defaultMessageStore.MessageStoreConfig.FlushDiskType {
		commitLog.FlushCommitLogService = new(GroupCommitService)
	} else {
		commitLog.FlushCommitLogService = NewFlushRealTimeService(commitLog)
	}

	commitLog.TopicQueueTable = make(map[string]int64, 1024)
	commitLog.AppendMessageCallback = NewDefaultAppendMessageCallback(defaultMessageStore.MessageStoreConfig.MaxMessageSize, commitLog)

	return commitLog
}

func (self *CommitLog) Load() bool {
	result := self.MapedFileQueue.load()

	if result {
		logger.Info("load commit log OK")
	} else {
		logger.Info("load commit log Failed")
	}

	return result
}

func (self *CommitLog) putMessage(msg *MessageExtBrokerInner) *PutMessageResult {
	msg.StoreTimestamp = time.Now().UnixNano() / 1000000
	// TOD0 crc32
	msg.BodyCRC = 0

	// TODO 事务消息处理
	self.mutex.Lock()
	beginLockTimestamp := time.Now().UnixNano() / 1000000
	msg.BornTimestamp = beginLockTimestamp

	mapedFile, err := self.MapedFileQueue.getLastMapedFile(int64(0))
	if err != nil {
		// TODO
		return &PutMessageResult{PutMessageStatus: CREATE_MAPEDFILE_FAILED}
	}

	if mapedFile == nil {
		// TODO
		return &PutMessageResult{PutMessageStatus: CREATE_MAPEDFILE_FAILED}
	}

	result := mapedFile.AppendMessageWithCallBack(msg, self.AppendMessageCallback)
	switch result.Status {
	case APPENDMESSAGE_PUT_OK:
		break
	case END_OF_FILE:
		mapedFile, err = self.MapedFileQueue.getLastMapedFile(int64(0))
		if err != nil {
			logger.Error(err.Error())
			return &PutMessageResult{PutMessageStatus: CREATE_MAPEDFILE_FAILED, AppendMessageResult: result}
		}

		if mapedFile == nil {
			logger.Errorf("create maped file2 error, topic:%s clientAddr:%s", msg.Topic, msg.BornHost)
			return &PutMessageResult{PutMessageStatus: CREATE_MAPEDFILE_FAILED, AppendMessageResult: result}
		}

		result = mapedFile.AppendMessageWithCallBack(msg, self.AppendMessageCallback)
		break
	case MESSAGE_SIZE_EXCEEDED:
		return &PutMessageResult{PutMessageStatus: MESSAGE_ILLEGAL, AppendMessageResult: result}
	default:
		return &PutMessageResult{PutMessageStatus: PUTMESSAGE_UNKNOWN_ERROR, AppendMessageResult: result}
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

	self.DefaultMessageStore.DispatchMessageService.putRequest(dispatchRequest)

	eclipseTimeInLock := time.Now().UnixNano()/1000000 - beginLockTimestamp
	self.mutex.Unlock()

	if eclipseTimeInLock > 1000 {
		logger.Warn("putMessage in lock eclipse time(ms) ", eclipseTimeInLock)
	}

	putMessageResult := &PutMessageResult{PutMessageStatus: PUTMESSAGE_PUT_OK, AppendMessageResult: result}

	// Statistics
	size := self.DefaultMessageStore.StoreStatsService.getSinglePutMessageTopicSizeTotal(msg.Topic)
	self.DefaultMessageStore.StoreStatsService.setSinglePutMessageTopicSizeTotal(msg.Topic, atomic.AddInt64(&size, result.WroteBytes))

	// Synchronization flush
	if config.SYNC_FLUSH == self.DefaultMessageStore.MessageStoreConfig.FlushDiskType {
		// TODO
	} else {
		//self.FlushCommitLogService
	}

	return putMessageResult
}

func (self *CommitLog) getMessage(offset int64, size int32) *SelectMapedBufferResult {
	returnFirstOnNotFound := false
	if 0 == offset {
		returnFirstOnNotFound = true
	}

	mapedFile := self.MapedFileQueue.findMapedFileByOffset(offset, returnFirstOnNotFound)
	if mapedFile != nil {
		mapedFileSize := self.DefaultMessageStore.MessageStoreConfig.MapedFileSizeCommitLog
		pos := offset % int64(mapedFileSize)
		result := mapedFile.selectMapedBufferByPosAndSize(pos, size)
		return result
	}

	return nil
}

func (self *CommitLog) rollNextFile(offset int64) int64 {
	mapedFileSize := self.DefaultMessageStore.MessageStoreConfig.MapedFileSizeCommitLog
	nextOffset := offset + int64(mapedFileSize) - offset%int64(mapedFileSize)
	return nextOffset
}

func (self *CommitLog) recoverNormally() {
	// checkCRCOnRecover := self.DefaultMessageStore.MessageStoreConfig.CheckCRCOnRecover
	mapedFiles := self.MapedFileQueue.mapedFiles
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
			dispatchRequest := self.checkMessageAndReturnSize(mappedByteBuffer,
				self.DefaultMessageStore.MessageStoreConfig.CheckCRCOnRecover, true)
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
					mappedByteBuffer = NewMappedByteBuffer(mapedFile.mappedByteBuffer.Bytes())
					processOffset = mapedFile.fileFromOffset
					mapedFileOffset = 0
				}
			}
		}

		processOffset += mapedFileOffset
		self.MapedFileQueue.committedWhere = processOffset
		self.MapedFileQueue.truncateDirtyFiles(processOffset)

	}
}

func (self *CommitLog) pickupStoretimestamp(offset int64, size int32) int64 {
	if offset > self.getMinOffset() {
		result := self.getMessage(offset, size)
		if result != nil {
			defer result.Release()
			result.MappedByteBuffer.ReadPos = message.MessageStoreTimestampPostion
			storeTimestamp := result.MappedByteBuffer.ReadInt64()
			return storeTimestamp
		}
	}

	return -1
}

func (self *CommitLog) getMinOffset() int64 {
	mapedFile := self.MapedFileQueue.getFirstMapedFileOnLock()
	if mapedFile != nil {
		// TODO mapedFile.isAvailable()
		return mapedFile.fileFromOffset
	}

	return -1
}

func (self *CommitLog) getMaxOffset() int64 {
	return self.MapedFileQueue.getMaxOffset()
}

func (self *CommitLog) removeQueurFromTopicQueueTable(topic string, queueId int32) {
	self.mutex.Lock()
	self.mutex.Unlock()
	key := fmt.Sprintf("%s-%d", topic, queueId)
	delete(self.TopicQueueTable, key)
}

func (self *CommitLog) checkMessageAndReturnSize(mappedByteBuffer *MappedByteBuffer, checkCRC bool, readBody bool) *DispatchRequest {
	// byteBufferMessage := self.AppendMessageCallback.msgStoreItemMemory
	totalSize := mappedByteBuffer.ReadInt32() // 1 TOTALSIZE
	magicCode := mappedByteBuffer.ReadInt32() // 2 MAGICCODE

	messageMagicCode := MessageMagicCode
	blankMagicCode := BlankMagicCode

	switch magicCode {
	case int32(messageMagicCode):
		break
	case int32(blankMagicCode):
		return &DispatchRequest{msgSize: 0}
	default:
		logger.Warn("found a illegal magic code ", magicCode)
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

	// 16 TOPIC
	topicLen := mappedByteBuffer.ReadInt8()
	topic := ""
	if topicLen > 0 {
		topicBytes := make([]byte, topicLen)
		mappedByteBuffer.Read(topicBytes)
		topic = string(topicBytes)
	}

	tagsCode := int64(0)
	keys := ""

	// 17 properties
	propertiesLength := mappedByteBuffer.ReadInt16()
	if propertiesLength > 0 {
		propertiesBytes := make([]byte, propertiesLength)
		mappedByteBuffer.Read(propertiesBytes)
		properties := string(propertiesBytes)
		propertiesMap := message.String2messageProperties(properties)
		keys = propertiesMap["PROPERTY_KEYS"]
		tags := propertiesMap["PROPERTY_TAGS"]
		if len(tags) > 0 {
			tagsCode = TagsString2tagsCode(message.ParseTopicFilterType(sysFlag), tags)
		}

		// Timing message processing
		delayLevelStr, ok := propertiesMap["PROPERTY_DELAY_TIME_LEVEL"]
		if SCHEDULE_TOPIC == topic && ok {
			delayLevelTemp, _ := strconv.Atoi(delayLevelStr)
			delayLevel := int32(delayLevelTemp)

			if delayLevel > self.DefaultMessageStore.ScheduleMessageService.maxDelayLevel {
				delayLevel = self.DefaultMessageStore.ScheduleMessageService.maxDelayLevel
			}

			if delayLevel > 0 {
				tagsCode = self.DefaultMessageStore.ScheduleMessageService.computeDeliverTimestamp(delayLevel, storeTimestamp)
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

func (self *CommitLog) recoverAbnormally() {
	checkCRCOnRecover := self.DefaultMessageStore.MessageStoreConfig.CheckCRCOnRecover
	mapedFiles := self.MapedFileQueue.mapedFiles
	if mapedFiles.Len() > 0 {
		// Looking beginning to recover from which file
		var mapedFile *MapedFile
		index := mapedFiles.Len() - 1
		for element := mapedFiles.Back(); element != nil; element = element.Prev() {
			index--
			mapedFile = element.Value.(*MapedFile)
			if self.isMapedFileMatchedRecover(mapedFile) {
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
				dispatchRequest := self.checkMessageAndReturnSize(mappedByteBuffer, checkCRCOnRecover, true)
				size := dispatchRequest.msgSize

				if size > 0 { // Normal data
					mapedFileOffset += size
					self.DefaultMessageStore.putDispatchRequest(dispatchRequest)
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
			self.MapedFileQueue.committedWhere = processOffset
			self.MapedFileQueue.truncateDirtyFiles(processOffset)

			// Clear ConsumeQueue redundant data
			self.DefaultMessageStore.truncateDirtyLogicFiles(processOffset)
		} else {
			// Commitlog case files are deleted
			self.MapedFileQueue.committedWhere = 0
			self.DefaultMessageStore.destroyLogics()
		}
	}
}

func (self *CommitLog) isMapedFileMatchedRecover(mapedFile *MapedFile) bool {
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

	if self.DefaultMessageStore.MessageStoreConfig.MessageIndexEnable &&
		self.DefaultMessageStore.MessageStoreConfig.MessageIndexSafe {
		if storeTimestamp <= self.DefaultMessageStore.StoreCheckpoint.getMinTimestampIndex() {
			logger.Infof("find check timestamp, %d %s", storeTimestamp,
				utils.TimeMillisecondToHumanString(time.Unix(storeTimestamp, 0)))
			return true
		}
	} else {
		if storeTimestamp <= self.DefaultMessageStore.StoreCheckpoint.getMinTimestamp() {
			logger.Infof("find check timestamp, %d %s", storeTimestamp,
				utils.TimeMillisecondToHumanString(time.Unix(storeTimestamp, 0)))
			return true
		}
	}

	return false
}

func (self *CommitLog) deleteExpiredFile(expiredTime int64, deleteFilesInterval int32, intervalForcibly int64, cleanImmediately bool) int {
	return self.MapedFileQueue.deleteExpiredFileByTime(expiredTime, int(deleteFilesInterval), intervalForcibly, cleanImmediately)
}

func (self *CommitLog) retryDeleteFirstFile(intervalForcibly int64) bool {
	return self.MapedFileQueue.retryDeleteFirstFile(intervalForcibly)
}

func (self *CommitLog) destroy() {
	if self.MapedFileQueue != nil {
		self.MapedFileQueue.destroy()
	}
}

func (self *CommitLog) Start() {
	// TODO self.FlushCommitLogService.start()
}

func (self *CommitLog) Shutdown() {
	// TODO self.FlushCommitLogService.shutdown()
}

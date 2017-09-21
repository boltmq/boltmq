package stgstorelog

import (
	"sync"
	"time"

	"bytes"
	"container/list"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
	"fmt"
)

const (
	MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8
	BlankMagicCode   = 0xBBCCDDEE ^ 1880681586 + 8
)

type CommitLog struct {
	MapedFileQueue        *MapedFileQueue
	DefaultMessageStore   *DefaultMessageStore
	FlushCommitLogService FlushCommitLogService
	AppendMessageCallback AppendMessageCallback
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

func (self *CommitLog) Start() {
	// TODO
}

func (self *CommitLog) putMessage(msg *MessageExtBrokerInner) *PutMessageResult {
	msg.StoreTimestamp = time.Now().Unix()
	// TOD0 crc32
	msg.BodyCRC = 0

	// TODO 事务消息处理
	self.mutex.Lock()
	beginLockTimestamp := time.Now().Unix()
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

	eclipseTimeInLock := time.Now().Unix() - beginLockTimestamp
	self.mutex.Unlock()

	if eclipseTimeInLock > 1000 {
		logger.Warn("putMessage in lock eclipse time(ms) ", eclipseTimeInLock)
	}

	putMessageResult := &PutMessageResult{PutMessageStatus: PUTMESSAGE_PUT_OK, AppendMessageResult: result}

	// TODO self.DefaultMessageStore.StoreStatsService
	// TODO

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

		var mapedFile *MapedFile
		var element *list.Element
		i := mapedFiles.Len()
		for element := mapedFiles.Back(); element != nil; element = element.Prev() {
			if i == index {
				mapedFile = element.Value.(*MapedFile)
			}
			i--
		}

		byteBuffer := mapedFile.mappedByteBuffer.slice()
		processOffset := mapedFile.fileFromOffset
		mapedFileOffset := int64(0)
		for {
			dispatchRequest := self.checkMessageAndReturnSize(byteBuffer,
				self.DefaultMessageStore.MessageStoreConfig.CheckCRCOnRecover, true)
			size := dispatchRequest.msgSize
			if size > 0 {
				mapedFileOffset += size
			} else if size == 1 {
				logger.Info("recover physics file end, ", mapedFile.fileName)
				break
			} else if size == 0 {
				index++
				if index >= mapedFiles.Len() {
					logger.Info("recover last 3 physics file over, last maped file ", mapedFile.fileName)
					break
				} else {
					mapedFile = element.Next().Value.(*MapedFile)
					byteBuffer = mapedFile.mappedByteBuffer.slice()
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
			result.MappedByteBuffer.ReadPos = message.MessageStoreTimestampPostion
			storeTimestamp := result.MappedByteBuffer.ReadInt64()
			result.MappedByteBuffer.unmap()
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

func (self *CommitLog) checkMessageAndReturnSize(byteBuffer *bytes.Buffer, checkCRC bool, readBody bool) *DispatchRequest {
	// TODO
	return nil
}

func (self *CommitLog) recoverAbnormally() {
	// TODO
}

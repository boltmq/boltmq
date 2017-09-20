package stgstorelog

import (
	"os"
	"path/filepath"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"bytes"
	"encoding/binary"
	"time"
	"strconv"
)

// ConsumeQueue 消费队列实现
// Author zhoufei
// Since 2017/9/7
const (
	CQStoreUnitSize = 20 // 存储单元大小
)

type ConsumeQueue struct {
	defaultMessageStore *DefaultMessageStore // 存储顶层对象
	mapedFileQueue      *MapedFileQueue      // 存储消息索引的队列
	topic               string
	queueId             int32
	byteBufferIndex     *MappedByteBuffer
	storePath           string
	mapedFileSize       int64
	maxPhysicOffset     int64 // 最后一个消息对应的物理Offset
	minLogicOffset      int64 // 逻辑队列的最小Offset，删除物理文件时，计算出来的最小Offset
}

func NewConsumeQueue(topic string, queueId int32, storePath string, mapedFileSize int64, defaultMessageStore *DefaultMessageStore) *ConsumeQueue {
	consumeQueue := new(ConsumeQueue)
	consumeQueue.storePath = storePath
	consumeQueue.mapedFileSize = mapedFileSize
	consumeQueue.maxPhysicOffset = -1
	consumeQueue.defaultMessageStore = defaultMessageStore
	consumeQueue.topic = topic
	consumeQueue.queueId = queueId

	pathSeparator := filepath.FromSlash(string(os.PathSeparator))

	queueDir := consumeQueue.storePath + pathSeparator + topic + pathSeparator + strconv.Itoa(int(queueId))

	consumeQueue.mapedFileQueue = NewMapedFileQueue(queueDir, mapedFileSize, nil)
	consumeQueue.byteBufferIndex = NewMappedByteBuffer(make([]byte, CQStoreUnitSize))

	return consumeQueue
}

func (self *ConsumeQueue) load() bool {
	result := self.mapedFileQueue.load()

	resultMsg := "Failed"
	if result {
		resultMsg = "OK"
	}

	logger.Infof("load consume queue %s-%d %s", self.topic, self.queueId, resultMsg)

	return result
}

func (self *ConsumeQueue) getIndexBuffer(startIndex int64) *SelectMapedBufferResult {
	offset := startIndex * int64(CQStoreUnitSize)

	if offset >= self.minLogicOffset {
		mapedFile := self.mapedFileQueue.findMapedFileByOffset(offset, false)
		if mapedFile != nil {
			result := mapedFile.selectMapedBuffer(offset % self.mapedFileSize)
			return result
		}
	}

	return nil
}

func (self *ConsumeQueue) rollNextFile(index int64) int64 {
	totalUnitsInFile := self.mapedFileSize / CQStoreUnitSize
	nextIndex := index + totalUnitsInFile - index%totalUnitsInFile
	return nextIndex
}

func (self *ConsumeQueue) recover() {
	/*
		mapedFiles := cq.mapedFileQueue.mapedFiles
		if mapedFiles.Len() > 0 {
			index := mapedFiles.Len() - 3
			if index < 0 {
				index = 0
			}
			var mapedFile *MapedFile

			i := mapedFiles.Len() - 1
			for e := mapedFiles.Back(); e != nil; e = e.Prev() {
				if index == i {
					mapedFile := e.Value.(MapedFile)
				}
			}

			// TODO
		}
	*/
}

func (selfcq *ConsumeQueue) getOffsetInQueueByTime(timestamp int64) {
	/*
		mapedFile := cq.mapedFileQueue.getMapedFileByTime(timestamp)
		if mapedFile != nil {
			offset := 0

			// 第一个索引信息的起始位置
			low := int64(0)
			if cq.minLogicOffset > mapedFile.fileFromOffset {
				low = cq.minLogicOffset - mapedFile.fileFromOffset
			}

			heigh := int64(0)
			midOffset, targetOffset, leftOffset, rightOffset := -1, -1, -1, -1
			leftIndexValue, rightIndexValue := int64(-1), int64(-1)

			// 取出该mapedFile里面所有的映射空间(没有映射的空间并不会返回,不会返回文件空洞)

		}
	*/
}

func (self *ConsumeQueue) getMinOffsetInQueue() int64 {
	return self.minLogicOffset / CQStoreUnitSize
}

func (self *ConsumeQueue) getMaxOffsetInQueue() int64 {
	return self.mapedFileQueue.getMaxOffset() / CQStoreUnitSize
}

func (self *ConsumeQueue) putMessagePostionInfoWrapper(offset, size, tagsCode, storeTimestamp, logicOffset int64) {
	maxRetries := 5
	//canWrite := self.defaultMessageStore.RunningFlags.isWriteable()
	for i := 0; i < maxRetries; i++ {
		result := self.putMessagePostionInfo(offset, size, tagsCode, logicOffset)
		if result {
			self.defaultMessageStore.StoreCheckpoint.logicsMsgTimestamp = storeTimestamp
			return
		} else {
			logger.Warnf("put commit log postion info to %s : %d failed, retry %d times %d",
				self.topic, self.queueId, offset, i)

			time.After(time.Duration(time.Millisecond * 1000))
		}
	}

	logger.Errorf("consume queue can not write %s %d", self.topic, self.queueId)
	self.defaultMessageStore.RunningFlags.makeLogicsQueueError()
}

func (self *ConsumeQueue) putMessagePostionInfo(offset, size, tagsCode, cqOffset int64) bool {
	if offset <= self.maxPhysicOffset {
		return true
	}

	self.byteBufferIndex.flip()
	self.byteBufferIndex.limit(CQStoreUnitSize)
	self.byteBufferIndex.WriteInt64(offset)
	self.byteBufferIndex.WriteInt32(int32(size))
	self.byteBufferIndex.WriteInt64(tagsCode)

	expectLogicOffset := cqOffset * CQStoreUnitSize
	mapedFile, err := self.mapedFileQueue.getLastMapedFile(expectLogicOffset)
	if err != nil {
		logger.Errorf("consume queue get last maped file error: %s", err.Error())
	}

	if mapedFile != nil {
		// 纠正MapedFile逻辑队列索引顺序
		if mapedFile.firstCreateInQueue && cqOffset != 0 && mapedFile.wrotePostion == 0 {
			self.minLogicOffset = expectLogicOffset
			self.fillPreBlank(mapedFile, expectLogicOffset)
			logger.Infof("fill pre blank space %s %d %d", mapedFile.fileName, expectLogicOffset, mapedFile.wrotePostion)
		}

		if cqOffset != 0 {
			currentLogicOffset := mapedFile.wrotePostion + mapedFile.fileFromOffset
			if expectLogicOffset != currentLogicOffset {
				logger.Warnf("logic queue order maybe wrong, expectLogicOffset: %d currentLogicOffset: %d Topic: %s QID: %d Diff: %d",
					expectLogicOffset, currentLogicOffset, self.topic, self.queueId, expectLogicOffset-currentLogicOffset)
			}
		}

		// 记录物理队列最大offset
		self.maxPhysicOffset = offset
		byteBuffers := self.byteBufferIndex.MMapBuf[:]
		return mapedFile.appendMessage(byteBuffers)
	}

	return false
}

func (self *ConsumeQueue) fillPreBlank(mapedFile *MapedFile, untilWhere int64) {
	byteBuffer := bytes.NewBuffer(make([]byte, CQStoreUnitSize))
	binary.Write(byteBuffer, binary.BigEndian, int64(0))
	binary.Write(byteBuffer, binary.BigEndian, int32(0x7fffffff))
	binary.Write(byteBuffer, binary.BigEndian, int64(0))

	until := int(untilWhere % self.mapedFileQueue.mapedFileSize)
	for i := 0; i < until; i++ {
		mapedFile.appendMessage(byteBuffer.Bytes())
	}
}

func (self *ConsumeQueue) commit(flushLeastPages int32) bool {
	return self.mapedFileQueue.commit(flushLeastPages)
}

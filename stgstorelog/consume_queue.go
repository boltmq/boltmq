package stgstorelog

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
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

	pathSeparator := GetPathSeparator()

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

func (self *ConsumeQueue) getOffsetInQueueByTime(timestamp int64) int64 {
	mapedFile := self.mapedFileQueue.getMapedFileByTime(timestamp)
	if mapedFile != nil {
		// 第一个索引信息的起始位置
		offset := 0
		high := 0
		low := 0
		if self.minLogicOffset > mapedFile.fileFromOffset {
			low = int(self.minLogicOffset - mapedFile.fileFromOffset)
		}

		midOffset, targetOffset, leftOffset, rightOffset := -1, -1, -1, -1
		leftIndexValue, rightIndexValue := int64(-1), int64(-1)

		// 取出该mapedFile里面所有的映射空间(没有映射的空间并不会返回,不会返回文件空洞)
		selectBuffer := mapedFile.selectMapedBuffer(0)
		if selectBuffer != nil {
			buffer := selectBuffer.MappedByteBuffer
			high = buffer.Limit - CQStoreUnitSize

			for {
				if high >= low {
					midOffset = (low + high) / (2 * CQStoreUnitSize) * CQStoreUnitSize
					buffer.ReadPos = midOffset
					phyOffset := buffer.ReadInt64()
					size := buffer.ReadInt32()

					storeTime := self.defaultMessageStore.CommitLog.pickupStoretimestamp(phyOffset, size)
					if storeTime < 0 { // 没有从物理文件找到消息，此时直接返回0
						return 0
					} else if storeTime == timestamp {
						targetOffset = midOffset
						break
					} else if storeTime > timestamp {
						high = midOffset - CQStoreUnitSize
						rightOffset = midOffset
						rightIndexValue = storeTime
					} else {
						low = midOffset + CQStoreUnitSize
						leftOffset = midOffset
						leftIndexValue = storeTime
					}
				}

				// 查询的时间正好是消息索引记录写入的时间
				if targetOffset != -1 {
					offset = targetOffset
				} else {
					// timestamp 时间小于该MapedFile中第一条记录记录的时间
					if leftIndexValue == -1 {
						offset = rightOffset
					} else if rightIndexValue == -1 { // timestamp 时间大于该MapedFile中最后一条记录记录的时间
						offset = leftOffset
					} else {
						// 取最接近timestamp的offset
						if math.Abs(float64(timestamp-leftIndexValue)) > math.Abs(float64(timestamp-rightIndexValue)) {
							offset = rightOffset
						} else {
							offset = leftOffset
						}
					}
				}

				return mapedFile.fileFromOffset + int64(offset)/CQStoreUnitSize
			}
		}
	}

	// 映射文件被标记为不可用时返回0
	return 0
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

func (self *ConsumeQueue) getLastOffset() int64 {
	physicsLastOffset := int64(-1) // 物理队列Offset
	mapedFile := self.mapedFileQueue.getLastMapedFile2()

	if mapedFile != nil {
		position := mapedFile.wrotePostion - CQStoreUnitSize // 找到写入位置对应的索引项的起始位置
		if position < 0 {
			position = 0
		}

		sliceByteBuffer := mapedFile.mappedByteBuffer.MMapBuf[position:]
		buffer := NewMappedByteBuffer(sliceByteBuffer)
		for i := 0; i < int(self.mapedFileSize); i += CQStoreUnitSize {
			offset := buffer.ReadInt64()
			size := buffer.ReadInt32()
			buffer.ReadInt64()

			if offset >= 0 && size > 0 {
				physicsLastOffset = offset + int64(size)
			}

		}
	}

	return physicsLastOffset
}

func (self *ConsumeQueue) destroy() {
	self.maxPhysicOffset = -1
	self.minLogicOffset = 0
	self.mapedFileQueue.destroy()
}

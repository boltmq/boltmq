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
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/boltmq/boltmq/store/core"
	"github.com/boltmq/common/logger"
)

// ConsumeQueue 消费队列实现
// Author zhoufei
// Since 2017/9/7
const (
	CQStoreUnitSize = 20 // 存储单元大小
)

type ConsumeQueue struct {
	defaultMessageStore *DefaultMessageStore   // 存储顶层对象
	mapedFileQueue      *core.MapedFileQueue   // 存储消息索引的队列
	topic               string                 // topic
	queueId             int32                  // 队列ID
	byteBufferIndex     *core.MappedByteBuffer // 索引文件内存映射
	storePath           string                 // 存储路径
	mapedFileSize       int64                  // 映射文件大小
	maxPhysicOffset     int64                  // 最后一个消息对应的物理Offset
	minLogicOffset      int64                  // 逻辑队列的最小Offset，删除物理文件时，计算出来的最小Offset
}

func newConsumeQueue(topic string, queueId int32, storePath string, mapedFileSize int64, defaultMessageStore *DefaultMessageStore) *ConsumeQueue {
	consumeQueue := new(ConsumeQueue)
	consumeQueue.storePath = storePath
	consumeQueue.mapedFileSize = mapedFileSize
	consumeQueue.maxPhysicOffset = -1
	consumeQueue.minLogicOffset = 0
	consumeQueue.defaultMessageStore = defaultMessageStore
	consumeQueue.topic = topic
	consumeQueue.queueId = queueId

	queueDir := fmt.Sprintf("%s%c%s%c%d", consumeQueue.storePath, os.PathSeparator, topic, os.PathSeparator, queueId)
	consumeQueue.mapedFileQueue = core.NewMapedFileQueue(queueDir, mapedFileSize, nil)
	consumeQueue.byteBufferIndex = core.NewMappedByteBuffer(make([]byte, CQStoreUnitSize))

	return consumeQueue
}

func (cq *ConsumeQueue) load() bool {
	result := cq.mapedFileQueue.Load()
	resultMsg := "Failed"
	if result {
		resultMsg = "OK"
	}

	logger.Infof("load consume queue %s-%d %s", cq.topic, cq.queueId, resultMsg)

	return result
}

func (cq *ConsumeQueue) getIndexBuffer(startIndex int64) *core.SelectMapedBufferResult {
	offset := startIndex * int64(CQStoreUnitSize)

	if offset >= cq.minLogicOffset {
		mapedFile := cq.mapedFileQueue.FindMapedFileByOffset(offset, false)
		if mapedFile != nil {
			result := mapedFile.SelectMapedBuffer(offset % cq.mapedFileSize)
			return result
		}
	}

	return nil
}

func (cq *ConsumeQueue) rollNextFile(index int64) int64 {
	totalUnitsInFile := cq.mapedFileSize / CQStoreUnitSize
	nextIndex := index + totalUnitsInFile - index%totalUnitsInFile
	return nextIndex
}

func (cq *ConsumeQueue) correctMinOffset(phyMinOffset int64) {
	mapedFile := cq.mapedFileQueue.GetFirstMapedFileOnLock()
	if mapedFile != nil {
		result := mapedFile.SelectMapedBuffer(0)
		if result != nil {
			defer result.Release()
			for i := 0; i < int(result.Size); i += CQStoreUnitSize {
				offsetPy := result.MappedByteBuffer.ReadInt64()
				result.MappedByteBuffer.ReadInt32()
				result.MappedByteBuffer.ReadInt64()

				if offsetPy >= phyMinOffset {
					cq.minLogicOffset = result.MapedFile.FileFromOffset() + int64(i)
					//logger.Infof("compute logics min offset: %d, topic: %s, queueId: %d",
					//	cq.getMinOffsetInQueue(), cq.topic, cq.queueId)
					break
				}
			}
		}
	}
}

func getMapedFileByIndex(mapedFiles *list.List, index int) *core.MapedFile {
	i := 0
	for e := mapedFiles.Front(); e != nil; e = e.Next() {
		if index == i {
			mapedFile := e.Value.(*core.MapedFile)
			return mapedFile
		}

		i++
	}

	return nil
}

func (cq *ConsumeQueue) recover() {
	mapedFiles := cq.mapedFileQueue.MapedFiles()
	if mapedFiles.Len() > 0 {
		index := mapedFiles.Len() - 3
		if index < 0 {
			index = 0
		}

		mapedFile := getMapedFileByIndex(mapedFiles, index)

		byteBuffer := bytes.NewBuffer(mapedFile.MByteBuffer.Bytes())
		processOffset := mapedFile.FileFromOffset()
		mapedFileOffset := int64(0)

		for {
			for i := 0; i < int(cq.mapedFileSize); i += CQStoreUnitSize {
				var (
					offset   int64 = 0
					size     int32 = 0
					tagsCode int64 = 0
				)

				if err := binary.Read(byteBuffer, binary.BigEndian, &offset); err != nil {
					logger.Error("consume queue recover maped file offset error:", err.Error())
				}

				if err := binary.Read(byteBuffer, binary.BigEndian, &size); err != nil {
					logger.Error("consume queue recover maped file size error:", err.Error())
				}

				if err := binary.Read(byteBuffer, binary.BigEndian, &tagsCode); err != nil {
					logger.Error("consume queue recover maped file tags code error:", err.Error())
				}

				// 说明当前存储单元有效
				// TODO 这样判断有效是否合理？
				if offset >= 0 && size > 0 {
					mapedFileOffset = int64(i) + CQStoreUnitSize
					cq.maxPhysicOffset = offset
				} else {
					logger.Infof("recover current consume queue file over, %s %d %d %d ",
						mapedFile.FileName(), offset, size, tagsCode)
					break
				}
			}

			// 走到文件末尾，切换至下一个文件
			if mapedFileOffset == cq.mapedFileSize {
				index++
				if index >= mapedFiles.Len() {
					logger.Info("recover last consume queue file over, last maped file ", mapedFile.FileName())
					break
				} else {
					mapedFile = getMapedFileByIndex(mapedFiles, index)
					byteBuffer = bytes.NewBuffer(mapedFile.MByteBuffer.Bytes())
					processOffset = mapedFile.FileFromOffset()
					mapedFileOffset = 0
					logger.Info("recover next consume queue file, ", mapedFile.FileName())
				}
			} else {
				logger.Infof("recover current consume queue queue over %s %d",
					mapedFile.FileName(), processOffset+mapedFileOffset)
				break
			}
		}

		processOffset += mapedFileOffset
		cq.mapedFileQueue.TruncateDirtyFiles(processOffset)

	}
}

func (cq *ConsumeQueue) getOffsetInQueueByTime(timestamp int64) int64 {
	mapedFile := cq.mapedFileQueue.GetMapedFileByTime(timestamp)
	if mapedFile != nil {
		// 第一个索引信息的起始位置
		offset := 0
		high := 0
		low := 0
		if cq.minLogicOffset > mapedFile.FileFromOffset() {
			low = int(cq.minLogicOffset - mapedFile.FileFromOffset())
		}

		midOffset, targetOffset, leftOffset, rightOffset := -1, -1, -1, -1
		leftIndexValue, rightIndexValue := int64(-1), int64(-1)

		// 取出该mapedFile里面所有的映射空间(没有映射的空间并不会返回,不会返回文件空洞)
		selectBuffer := mapedFile.SelectMapedBuffer(0)
		if selectBuffer != nil {
			defer selectBuffer.Release()

			buffer := selectBuffer.MappedByteBuffer
			high = buffer.WritePos - CQStoreUnitSize

			for {
				if high < low {
					break
				}

				midOffset = (low + high) / (2 * CQStoreUnitSize) * CQStoreUnitSize
				buffer.ReadPos = midOffset
				phyOffset := buffer.ReadInt64()
				size := buffer.ReadInt32()

				storeTime := cq.defaultMessageStore.commitLog.pickupStoretimestamp(phyOffset, size)
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

			return (mapedFile.FileFromOffset() + int64(offset)) / CQStoreUnitSize

		}
	}

	// 映射文件被标记为不可用时返回0
	return 0
}

func (cq *ConsumeQueue) getMinOffsetInQueue() int64 {
	return cq.minLogicOffset / CQStoreUnitSize
}

func (cq *ConsumeQueue) getMaxOffsetInQueue() int64 {
	return cq.mapedFileQueue.GetMaxOffset() / CQStoreUnitSize
}

func (cq *ConsumeQueue) putMessagePostionInfoWrapper(offset, size, tagsCode, storeTimestamp, logicOffset int64) {
	maxRetries := 5
	//canWrite := cq.defaultMessageStore.RunningFlags.isWriteable()
	for i := 0; i < maxRetries; i++ {
		result := cq.putMessagePostionInfo(offset, size, tagsCode, logicOffset)
		if result {
			cq.defaultMessageStore.StoreCheckpoint.logicsMsgTimestamp = storeTimestamp
			return
		} else {
			logger.Warnf("put commit log postion info to %s : %d failed, retry %d times %d",
				cq.topic, cq.queueId, offset, i)

			time.After(time.Duration(time.Millisecond * 1000))
		}
	}

	logger.Errorf("consume queue can not write %s %d", cq.topic, cq.queueId)
	cq.defaultMessageStore.RunningFlags.makeLogicsQueueError()
}

func (cq *ConsumeQueue) putMessagePostionInfo(offset, size, tagsCode, cqOffset int64) bool {
	if offset <= cq.maxPhysicOffset {
		return true
	}

	cq.resetMsgStoreItemMemory(CQStoreUnitSize)
	cq.byteBufferIndex.WriteInt64(offset)
	cq.byteBufferIndex.WriteInt32(int32(size))
	cq.byteBufferIndex.WriteInt64(tagsCode)

	expectLogicOffset := cqOffset * CQStoreUnitSize
	mapedFile, err := cq.mapedFileQueue.GetLastMapedFile(expectLogicOffset)
	if err != nil {
		logger.Errorf("consume queue get last maped file error: %s", err.Error())
	}

	if mapedFile != nil {
		// 纠正MapedFile逻辑队列索引顺序
		if mapedFile.IsFirstCreateInQueue() && cqOffset != 0 && mapedFile.WrotePostion == 0 {
			cq.minLogicOffset = expectLogicOffset
			cq.fillPreBlank(mapedFile, expectLogicOffset)
			logger.Infof("fill pre blank space %s %d %d", mapedFile.FileName(), expectLogicOffset, mapedFile.WrotePostion)
		}

		if cqOffset != 0 {
			currentLogicOffset := mapedFile.WrotePostion + mapedFile.FileFromOffset()
			if expectLogicOffset != currentLogicOffset {
				logger.Warnf("logic queue order maybe wrong, expectLogicOffset: %d currentLogicOffset: %d Topic: %s QID: %d Diff: %d",
					expectLogicOffset, currentLogicOffset, cq.topic, cq.queueId, expectLogicOffset-currentLogicOffset)
			}
		}

		// 记录物理队列最大offset
		cq.maxPhysicOffset = offset
		byteBuffers := cq.byteBufferIndex.Bytes()
		return mapedFile.AppendMessage(byteBuffers)
	}

	return false
}

func (cq *ConsumeQueue) fillPreBlank(mapedFile *core.MapedFile, untilWhere int64) {
	byteBuffer := bytes.NewBuffer(make([]byte, CQStoreUnitSize))
	binary.Write(byteBuffer, binary.BigEndian, int64(0))
	binary.Write(byteBuffer, binary.BigEndian, int32(0x7fffffff))
	binary.Write(byteBuffer, binary.BigEndian, int64(0))
	dataBuffer := make([]byte, CQStoreUnitSize)
	byteBuffer.Read(dataBuffer)

	until := int(untilWhere % cq.mapedFileQueue.MapedFileSize())
	for i := 0; i < until; i += CQStoreUnitSize {
		mapedFile.AppendMessage(dataBuffer)
	}
}

func (cq *ConsumeQueue) commit(flushLeastPages int32) bool {
	return cq.mapedFileQueue.Commit(flushLeastPages)
}

func (cq *ConsumeQueue) getLastOffset() int64 {
	physicsLastOffset := int64(-1) // 物理队列Offset
	mapedFile := cq.mapedFileQueue.GetLastMapedFile2()

	if mapedFile != nil {
		position := mapedFile.WrotePostion - CQStoreUnitSize // 找到写入位置对应的索引项的起始位置
		if position < 0 {
			position = 0
		}

		sliceByteBuffer := mapedFile.MByteBuffer.MMapBuf[position:]
		buffer := core.NewMappedByteBuffer(sliceByteBuffer)
		for i := 0; i < int(cq.mapedFileSize); i += CQStoreUnitSize {
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

func (cq *ConsumeQueue) truncateDirtyLogicFiles(phyOffet int64) {
	logicFileSize := int(cq.mapedFileSize)

	for {
		mapedFile := cq.mapedFileQueue.GetLastMapedFile2()
		if mapedFile != nil {
			mappedBuyteBuffer := core.NewMappedByteBuffer(mapedFile.MByteBuffer.Bytes())
			mapedFile.WrotePostion = 0
			mapedFile.CommittedPosition = 0

			for i := 0; i < logicFileSize; i += CQStoreUnitSize {
				offset := mappedBuyteBuffer.ReadInt64()
				size := mappedBuyteBuffer.ReadInt32()
				mappedBuyteBuffer.ReadInt64()

				if 0 == i {
					if offset >= phyOffet {
						cq.mapedFileQueue.DeleteLastMapedFile()
						break
					} else {
						pos := i + CQStoreUnitSize
						mapedFile.WrotePostion = int64(pos)
						mapedFile.CommittedPosition = int64(pos)
						cq.maxPhysicOffset = offset
					}
				} else {
					if offset >= 0 && size > 0 {
						if offset >= phyOffet {
							return
						}

						pos := i + CQStoreUnitSize
						mapedFile.WrotePostion = int64(pos)
						mapedFile.CommittedPosition = int64(pos)
						cq.maxPhysicOffset = offset

						if pos == logicFileSize {
							return
						}
					} else {
						return
					}
				}
			}
		} else {
			break
		}
	}
}

func (cq *ConsumeQueue) destroy() {
	cq.maxPhysicOffset = -1
	cq.minLogicOffset = 0
	cq.mapedFileQueue.Destroy()
}

func (cq *ConsumeQueue) resetMsgStoreItemMemory(length int32) {
	cq.byteBufferIndex.Limit = cq.byteBufferIndex.WritePos
	cq.byteBufferIndex.WritePos = 0
	cq.byteBufferIndex.Limit = int(length)
	if cq.byteBufferIndex.WritePos > cq.byteBufferIndex.Limit {
		cq.byteBufferIndex.WritePos = cq.byteBufferIndex.Limit
	}
}

func (cq *ConsumeQueue) deleteExpiredFile(offset int64) int {
	count := cq.mapedFileQueue.DeleteExpiredFileByOffset(offset, CQStoreUnitSize)
	cq.correctMinOffset(offset)
	return count
}

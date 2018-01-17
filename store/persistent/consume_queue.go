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
	"math"
	"os"
	"time"

	"github.com/boltmq/common/logger"
)

type consumeQueue struct {
	messageStore    *PersistentMessageStore // 存储顶层对象
	mfq             *mappedFileQueue        // 存储消息索引的队列
	topic           string                  // topic
	queueId         int32                   // 队列ID
	byteBufferIndex *mappedByteBuffer       // 索引文件内存映射
	storePath       string                  // 存储路径
	mfSize          int64                   // 映射文件大小
	maxPhysicOffset int64                   // 最后一个消息对应的物理Offset
	minLogicOffset  int64                   // 逻辑队列的最小Offset，删除物理文件时，计算出来的最小Offset
}

func newConsumeQueue(topic string, queueId int32, storePath string, mfSize int64, messageStore *PersistentMessageStore) *consumeQueue {
	cq := &consumeQueue{}
	cq.storePath = storePath
	cq.mfSize = mfSize
	cq.maxPhysicOffset = -1
	cq.minLogicOffset = 0
	cq.messageStore = messageStore
	cq.topic = topic
	cq.queueId = queueId

	queueDir := fmt.Sprintf("%s%c%s%c%d", cq.storePath, os.PathSeparator, topic, os.PathSeparator, queueId)
	cq.mfq = newMappedFileQueue(queueDir, mfSize, nil)
	cq.byteBufferIndex = newMappedByteBuffer(make([]byte, CQStoreUnitSize))

	return cq
}

func (cq *consumeQueue) load() bool {
	result := cq.mfq.load()
	resultMsg := "failed"
	if result {
		resultMsg = "success"
	}

	logger.Infof("load consumequeue %s-%d %s.", cq.topic, cq.queueId, resultMsg)
	return result
}

func (cq *consumeQueue) getIndexBuffer(startIndex int64) *mappedBufferResult {
	offset := startIndex * int64(CQStoreUnitSize)

	if offset >= cq.minLogicOffset {
		mf := cq.mfq.findMappedFileByOffset(offset, false)
		if mf != nil {
			result := mf.selectMappedBuffer(offset % cq.mfSize)
			return result
		}
	}

	return nil
}

func (cq *consumeQueue) rollNextFile(index int64) int64 {
	totalUnitsInFile := cq.mfSize / CQStoreUnitSize
	nextIndex := index + totalUnitsInFile - index%totalUnitsInFile
	return nextIndex
}

func (cq *consumeQueue) correctMinOffset(phyMinOffset int64) {
	mf := cq.mfq.getFirstMappedFileOnLock()
	if mf != nil {
		result := mf.selectMappedBuffer(0)
		if result != nil {
			defer result.Release()
			for i := 0; i < int(result.size); i += CQStoreUnitSize {
				offsetPy := result.byteBuffer.ReadInt64()
				result.byteBuffer.ReadInt32()
				result.byteBuffer.ReadInt64()

				if offsetPy >= phyMinOffset {
					cq.minLogicOffset = result.mfile.fileFromOffset + int64(i)
					//logger.Infof("compute logics min offset: %d, topic: %s, queueId: %d",
					//	cq.getMinOffsetInQueue(), cq.topic, cq.queueId)
					break
				}
			}
		}
	}
}

func getMappedFileByIndex(mfs *list.List, index int) *mappedFile {
	i := 0
	for e := mfs.Front(); e != nil; e = e.Next() {
		if index == i {
			mf := e.Value.(*mappedFile)
			return mf
		}

		i++
	}

	return nil
}

func (cq *consumeQueue) recover() {
	mfs := cq.mfq.mappedFiles
	if mfs.Len() > 0 {
		index := mfs.Len() - 3
		if index < 0 {
			index = 0
		}

		mf := getMappedFileByIndex(mfs, index)

		byteBuffer := bytes.NewBuffer(mf.byteBuffer.Bytes())
		processOffset := mf.fileFromOffset
		mfOffset := int64(0)

		for {
			for i := 0; i < int(cq.mfSize); i += CQStoreUnitSize {
				var (
					offset   int64 = 0
					size     int32 = 0
					tagsCode int64 = 0
				)

				if err := binary.Read(byteBuffer, binary.BigEndian, &offset); err != nil {
					logger.Errorf("consumequeue recover mapped file offset err: %s.", err)
				}

				if err := binary.Read(byteBuffer, binary.BigEndian, &size); err != nil {
					logger.Errorf("consumequeue recover mapped file size err: %s.", err)
				}

				if err := binary.Read(byteBuffer, binary.BigEndian, &tagsCode); err != nil {
					logger.Errorf("consumequeue recover mapped file tags code err: %s.", err)
				}

				// 说明当前存储单元有效
				// TODO 这样判断有效是否合理？
				if offset >= 0 && size > 0 {
					mfOffset = int64(i) + CQStoreUnitSize
					cq.maxPhysicOffset = offset
				} else {
					logger.Infof("recover current consumequeue file over, %s %d %d %d.",
						mf.fileName, offset, size, tagsCode)
					break
				}
			}

			// 走到文件末尾，切换至下一个文件
			if mfOffset == cq.mfSize {
				index++
				if index >= mfs.Len() {
					logger.Infof("recover last consumequeue file over, last mapped file %s.", mf.fileName)
					break
				} else {
					mf = getMappedFileByIndex(mfs, index)
					byteBuffer = bytes.NewBuffer(mf.byteBuffer.Bytes())
					processOffset = mf.fileFromOffset
					mfOffset = 0
					logger.Infof("recover next consumequeue file, %s.", mf.fileName)
				}
			} else {
				logger.Infof("recover current consumequeue over %s %d.",
					mf.fileName, processOffset+mfOffset)
				break
			}
		}

		processOffset += mfOffset
		cq.mfq.truncateDirtyFiles(processOffset)

	}
}

func (cq *consumeQueue) getOffsetInQueueByTime(timestamp int64) int64 {
	mf := cq.mfq.getMappedFileByTime(timestamp)
	if mf != nil {
		// 第一个索引信息的起始位置
		offset := 0
		high := 0
		low := 0
		if cq.minLogicOffset > mf.fileFromOffset {
			low = int(cq.minLogicOffset - mf.fileFromOffset)
		}

		midOffset, targetOffset, leftOffset, rightOffset := -1, -1, -1, -1
		leftIndexValue, rightIndexValue := int64(-1), int64(-1)

		// 取出该mf里面所有的映射空间(没有映射的空间并不会返回,不会返回文件空洞)
		selectBuffer := mf.selectMappedBuffer(0)
		if selectBuffer != nil {
			defer selectBuffer.Release()

			buffer := selectBuffer.byteBuffer
			high = buffer.writePos - CQStoreUnitSize

			for {
				if high < low {
					break
				}

				midOffset = (low + high) / (2 * CQStoreUnitSize) * CQStoreUnitSize
				buffer.readPos = midOffset
				phyOffset := buffer.ReadInt64()
				size := buffer.ReadInt32()

				storeTime := cq.messageStore.clog.pickupStoretimestamp(phyOffset, size)
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
				// timestamp 时间小于该MappedFile中第一条记录记录的时间
				if leftIndexValue == -1 {
					offset = rightOffset
				} else if rightIndexValue == -1 { // timestamp 时间大于该MappedFile中最后一条记录记录的时间
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

			return (mf.fileFromOffset + int64(offset)) / CQStoreUnitSize

		}
	}

	// 映射文件被标记为不可用时返回0
	return 0
}

func (cq *consumeQueue) getMinOffsetInQueue() int64 {
	return cq.minLogicOffset / CQStoreUnitSize
}

func (cq *consumeQueue) getMaxOffsetInQueue() int64 {
	return cq.mfq.getMaxOffset() / CQStoreUnitSize
}

func (cq *consumeQueue) putMessagePostionInfoWrapper(offset, size, tagsCode, storeTimestamp, logicOffset int64) {
	maxRetries := 5
	//canWrite := cq.messageStore.runFlags.isWriteable()
	for i := 0; i < maxRetries; i++ {
		result := cq.putMessagePostionInfo(offset, size, tagsCode, logicOffset)
		if result {
			cq.messageStore.steCheckpoint.logicsMsgTimestamp = storeTimestamp
			return
		} else {
			logger.Warnf("put commit log postion info to %s : %d failed, retry %d times %d",
				cq.topic, cq.queueId, offset, i)

			time.After(time.Duration(time.Millisecond * 1000))
		}
	}

	logger.Errorf("consumequeue can not write %s %d.", cq.topic, cq.queueId)
	cq.messageStore.runFlags.makeLogicsQueueError()
}

func (cq *consumeQueue) putMessagePostionInfo(offset, size, tagsCode, cqOffset int64) bool {
	if offset <= cq.maxPhysicOffset {
		return true
	}

	cq.resetMsgStoreItemMemory(CQStoreUnitSize)
	cq.byteBufferIndex.WriteInt64(offset)
	cq.byteBufferIndex.WriteInt32(int32(size))
	cq.byteBufferIndex.WriteInt64(tagsCode)

	expectLogicOffset := cqOffset * CQStoreUnitSize
	mf, err := cq.mfq.getLastMappedFile(expectLogicOffset)
	if err != nil {
		logger.Errorf("consumequeue get last mapped file error: %s.", err)
	}

	if mf != nil {
		// 纠正MappedFile逻辑队列索引顺序
		if mf.firstCreateInQueue && cqOffset != 0 && mf.wrotePostion == 0 {
			cq.minLogicOffset = expectLogicOffset
			cq.fillPreBlank(mf, expectLogicOffset)
			logger.Infof("fill pre blank space %s %d %d.", mf.fileName, expectLogicOffset, mf.wrotePostion)
		}

		if cqOffset != 0 {
			currentLogicOffset := mf.wrotePostion + mf.fileFromOffset
			if expectLogicOffset != currentLogicOffset {
				logger.Warnf("logic queue order maybe wrong, expectLogicOffset: %d currentLogicOffset: %d Topic: %s QID: %d Diff: %d.",
					expectLogicOffset, currentLogicOffset, cq.topic, cq.queueId, expectLogicOffset-currentLogicOffset)
			}
		}

		// 记录物理队列最大offset
		cq.maxPhysicOffset = offset
		byteBuffers := cq.byteBufferIndex.Bytes()
		return mf.appendMessage(byteBuffers)
	}

	return false
}

func (cq *consumeQueue) fillPreBlank(mf *mappedFile, untilWhere int64) {
	byteBuffer := bytes.NewBuffer(make([]byte, CQStoreUnitSize))
	binary.Write(byteBuffer, binary.BigEndian, int64(0))
	binary.Write(byteBuffer, binary.BigEndian, int32(0x7fffffff))
	binary.Write(byteBuffer, binary.BigEndian, int64(0))
	dataBuffer := make([]byte, CQStoreUnitSize)
	byteBuffer.Read(dataBuffer)

	until := int(untilWhere % cq.mfq.mappedFileSize)
	for i := 0; i < until; i += CQStoreUnitSize {
		mf.appendMessage(dataBuffer)
	}
}

func (cq *consumeQueue) commit(flushLeastPages int32) bool {
	return cq.mfq.commit(flushLeastPages)
}

func (cq *consumeQueue) getLastOffset() int64 {
	physicsLastOffset := int64(-1) // 物理队列Offset
	mf := cq.mfq.getLastMappedFile2()

	if mf != nil {
		position := mf.wrotePostion - CQStoreUnitSize // 找到写入位置对应的索引项的起始位置
		if position < 0 {
			position = 0
		}

		sliceByteBuffer := mf.byteBuffer.mmapBuf[position:]
		buffer := newMappedByteBuffer(sliceByteBuffer)
		for i := 0; i < int(cq.mfSize); i += CQStoreUnitSize {
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

func (cq *consumeQueue) truncateDirtyLogicFiles(phyOffet int64) {
	logicFileSize := int(cq.mfSize)

	for {
		mf := cq.mfq.getLastMappedFile2()
		if mf != nil {
			mappedBuyteBuffer := newMappedByteBuffer(mf.byteBuffer.Bytes())
			mf.wrotePostion = 0
			mf.committedPosition = 0

			for i := 0; i < logicFileSize; i += CQStoreUnitSize {
				offset := mappedBuyteBuffer.ReadInt64()
				size := mappedBuyteBuffer.ReadInt32()
				mappedBuyteBuffer.ReadInt64()

				if 0 == i {
					if offset >= phyOffet {
						cq.mfq.deleteLastMappedFile()
						break
					} else {
						pos := i + CQStoreUnitSize
						mf.wrotePostion = int64(pos)
						mf.committedPosition = int64(pos)
						cq.maxPhysicOffset = offset
					}
				} else {
					if offset >= 0 && size > 0 {
						if offset >= phyOffet {
							return
						}

						pos := i + CQStoreUnitSize
						mf.wrotePostion = int64(pos)
						mf.committedPosition = int64(pos)
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

func (cq *consumeQueue) destroy() {
	cq.maxPhysicOffset = -1
	cq.minLogicOffset = 0
	cq.mfq.destroy()
}

func (cq *consumeQueue) resetMsgStoreItemMemory(length int32) {
	cq.byteBufferIndex.limit = cq.byteBufferIndex.writePos
	cq.byteBufferIndex.writePos = 0
	cq.byteBufferIndex.limit = int(length)
	if cq.byteBufferIndex.writePos > cq.byteBufferIndex.limit {
		cq.byteBufferIndex.writePos = cq.byteBufferIndex.limit
	}
}

func (cq *consumeQueue) deleteExpiredFile(offset int64) int {
	count := cq.mfq.deleteExpiredFileByOffset(offset, CQStoreUnitSize)
	cq.correctMinOffset(offset)
	return count
}

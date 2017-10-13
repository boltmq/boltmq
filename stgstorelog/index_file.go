package stgstorelog

import (
	"time"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"math"
	"hash/fnv"
)

var (
	HASH_SLOT_SIZE int32 = 4
	INDEX_SIZE     int32 = 20
	INVALID_INDEX  int32 = 0
)

type IndexFile struct {
	hashSlotNum      int32
	indexNum         int32
	mapedFile        *MapedFile
	mappedByteBuffer *MappedByteBuffer
	indexHeader      *IndexHeader
}

func NewIndexFile(fileName string, hashSlotNum, indexNum int32, endPhyOffset, endTimestamp int64) *IndexFile {
	fileTotalSize := INDEX_HEADER_SIZE + (hashSlotNum * HASH_SLOT_SIZE) + (indexNum * INDEX_SIZE)

	indexFile := new(IndexFile)
	mapedFile, err := NewMapedFile(fileName, int64(fileTotalSize))
	if err != nil {
		// TODO
	}

	indexFile.mapedFile = mapedFile
	indexFile.mappedByteBuffer = indexFile.mapedFile.mappedByteBuffer
	indexFile.hashSlotNum = hashSlotNum
	indexFile.indexNum = indexNum

	indexFile.indexHeader = NewIndexHeader(NewMappedByteBuffer(make([]byte, indexFile.mapedFile.fileSize)))

	if endPhyOffset > 0 {
		indexFile.indexHeader.setBeginPhyOffset(endPhyOffset)
		indexFile.indexHeader.setEndPhyOffset(endPhyOffset)
	}

	if endTimestamp > 0 {
		indexFile.indexHeader.setBeginTimestamp(endTimestamp)
		indexFile.indexHeader.setEndTimestamp(endTimestamp)
	}

	return indexFile
}

func (self *IndexFile) load() {
	self.indexHeader.load()
}

func (self *IndexFile) flush() {
	beginTime := time.Now().UnixNano() / 1000000
	self.indexHeader.updateByteBuffer()
	self.mappedByteBuffer.flush()
	self.mapedFile.release()
	endTime := time.Now().UnixNano() / 1000000
	logger.Info("flush index file eclipse time(ms) ", endTime-beginTime)
}

func (self *IndexFile) isWriteFull() bool {
	return self.indexHeader.indexCount >= self.indexNum
}

func (self *IndexFile) getEndPhyOffset() int64 {
	return self.indexHeader.endPhyOffset
}

func (self *IndexFile) getEndTimestamp() int64 {
	return self.indexHeader.endTimestamp
}

func (self *IndexFile) putKey(key string, phyOffset int64, storeTimestamp int64) bool {
	if self.indexHeader.indexCount < self.indexNum {
		keyHash := self.indexKeyHashMethod(key)
		slotPos := keyHash % self.hashSlotNum
		absSlotPos := INDEX_HEADER_SIZE + slotPos*HASH_SLOT_SIZE

		self.mappedByteBuffer.ReadPos = int(absSlotPos)
		slotValue := self.mappedByteBuffer.ReadInt32()
		if slotValue <= INVALID_INDEX || slotValue > self.indexHeader.indexCount {
			slotValue = INVALID_INDEX
		}

		timeDiff := storeTimestamp - self.indexHeader.beginTimestamp
		// 时间差存储单位由毫秒改为秒
		timeDiff = timeDiff / 1000

		if self.indexHeader.beginTimestamp <= 0 {
			timeDiff = 0
		} else if timeDiff > 0x7fffffff {
			timeDiff = 0x7fffffff
		} else if timeDiff < 0 {
			timeDiff = 0
		}

		absIndexPos := INDEX_HEADER_SIZE + self.hashSlotNum*HASH_SLOT_SIZE + self.indexHeader.indexCount*INDEX_SIZE

		// 写入真正索引
		self.mappedByteBuffer.WritePos = int(absIndexPos)
		self.mappedByteBuffer.WriteInt32(keyHash)
		self.mappedByteBuffer.WriteInt64(phyOffset)
		self.mappedByteBuffer.WriteInt32(int32(timeDiff))
		self.mappedByteBuffer.WriteInt32(slotValue)

		// 更新哈希槽
		currentWritePos := self.mappedByteBuffer.WritePos
		self.mappedByteBuffer.WritePos = int(absIndexPos)
		self.mappedByteBuffer.WriteInt32(self.indexHeader.indexCount)
		self.mappedByteBuffer.WritePos = currentWritePos

		// 第一次写入
		if self.indexHeader.indexCount <= 1 {
			self.indexHeader.beginPhyOffset = phyOffset
			self.indexHeader.beginTimestamp = storeTimestamp
		}

		self.indexHeader.incHashSlotCount()
		self.indexHeader.incIndexCount()
		self.indexHeader.endPhyOffset = phyOffset
		self.indexHeader.endTimestamp = storeTimestamp

		return true
	}

	return false
}

func (self *IndexFile) indexKeyHashMethod(key string) int32 {
	keyHash := self.indexKeyHashCode(key)
	keyHashPositive := math.Abs(float64(keyHash))
	if keyHashPositive < 0 {
		keyHashPositive = 0
	}
	return int32(keyHashPositive)
}

func (self *IndexFile) indexKeyHashCode(key string) int32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int32(h.Sum32())
}

func (self *IndexFile) destroy(intervalForcibly int64) bool {
	return self.mapedFile.destroy()
}

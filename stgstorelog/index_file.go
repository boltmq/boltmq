package stgstorelog

import (
	"time"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
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
	// TODO FileChannel fileChannel
	indexHeader *IndexHeader
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

	byteBuffer := indexFile.mappedByteBuffer.slice()
	indexFile.indexHeader = NewIndexHeader(NewMappedByteBuffer(byteBuffer.Bytes()))

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
	beginTime := time.Now().Unix()
	self.indexHeader.updateByteBuffer()
	self.mappedByteBuffer.flush()
	self.mapedFile.release()
	logger.Info("flush index file eclipse time(ms) ", (time.Now().Unix() - beginTime))
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

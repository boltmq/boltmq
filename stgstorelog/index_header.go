package stgstorelog

import (
	"sync/atomic"
)

var (
	INDEX_HEADER_SIZE    int32 = 40
	BEGINTIMESTAMP_INDEX int32 = 0
	ENDTIMESTAMP_INDEX   int32 = 8
	BEGINPHYOFFSET_INDEX int32 = 16
	ENDPHYOFFSET_INDEX   int32 = 24
	HASHSLOTCOUNT_INDEX  int32 = 32
	INDEXCOUNT_INDEX     int32 = 36
)

type IndexHeader struct {
	mappedByteBuffer *MappedByteBuffer
	beginTimestamp   int64
	endTimestamp     int64
	beginPhyOffset   int64
	endPhyOffset     int64
	hashSlotCount    int32
	indexCount       int32
}

func NewIndexHeader(mappedByteBuffer *MappedByteBuffer) *IndexHeader {
	indexHeader := new(IndexHeader)
	indexHeader.mappedByteBuffer = mappedByteBuffer
	return indexHeader
}

func (self *IndexHeader) load() {
	self.mappedByteBuffer.ReadPos = int(BEGINTIMESTAMP_INDEX)
	atomic.StoreInt64(&self.beginTimestamp, self.mappedByteBuffer.ReadInt64())
	atomic.StoreInt64(&self.endTimestamp, self.mappedByteBuffer.ReadInt64())
	atomic.StoreInt64(&self.beginPhyOffset, self.mappedByteBuffer.ReadInt64())
	atomic.StoreInt64(&self.endPhyOffset, self.mappedByteBuffer.ReadInt64())
	atomic.StoreInt32(&self.hashSlotCount, self.mappedByteBuffer.ReadInt32())
	atomic.StoreInt32(&self.indexCount, self.mappedByteBuffer.ReadInt32())

	if self.indexCount <= 0 {
		atomic.AddInt32(&self.indexCount, 1)
	}
}

func (self *IndexHeader) updateByteBuffer() {
	self.mappedByteBuffer.WritePos = int(BEGINTIMESTAMP_INDEX)
	self.mappedByteBuffer.WriteInt64(self.beginTimestamp)
	self.mappedByteBuffer.WriteInt64(self.endTimestamp)
	self.mappedByteBuffer.WriteInt64(self.beginPhyOffset)
	self.mappedByteBuffer.WriteInt64(self.endPhyOffset)
	self.mappedByteBuffer.WriteInt32(self.hashSlotCount)
	self.mappedByteBuffer.WriteInt32(self.indexCount)
}

func (self *IndexHeader) setBeginTimestamp(beginTimestamp int64) {
	self.beginTimestamp = beginTimestamp
	self.mappedByteBuffer.WriteInt64(beginTimestamp)
}

func (self *IndexHeader) setEndTimestamp(endTimestamp int64) {
	self.endTimestamp = endTimestamp
	self.mappedByteBuffer.WriteInt64(endTimestamp)
}

func (self *IndexHeader) setBeginPhyOffset(beginPhyOffset int64) {
	self.beginPhyOffset = beginPhyOffset
	self.mappedByteBuffer.WriteInt64(beginPhyOffset)
}

func (self *IndexHeader) setEndPhyOffset(endPhyOffset int64) {
	self.endPhyOffset = endPhyOffset
	self.mappedByteBuffer.WriteInt64(endPhyOffset)
}

func (self *IndexHeader) incHashSlotCount() {
	value := atomic.AddInt32(&self.hashSlotCount, int32(1))
	self.mappedByteBuffer.WriteInt32(value)
}

func (self *IndexHeader) incIndexCount() {
	value := atomic.AddInt32(&self.indexCount, int32(1))
	self.mappedByteBuffer.WriteInt32(value)
}

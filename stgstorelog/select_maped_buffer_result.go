package stgstorelog

import (
	"sync"
)

// SelectMapedBufferResult 查询Pagecache返回结果
// Author zhoufei
// Since 2017/9/6
type SelectMapedBufferResult struct {
	StartOffset      int64
	MappedByteBuffer *MappedByteBuffer
	Size             int32
	MapedFile        *MapedFile
	mutex            *sync.Mutex
}

func NewSelectMapedBufferResult(startOffset int64, mappedByteBuffer *MappedByteBuffer, size int32, mapedFile *MapedFile) *SelectMapedBufferResult {
	return &SelectMapedBufferResult{
		StartOffset:      startOffset,
		MappedByteBuffer: mappedByteBuffer,
		Size:             size,
		MapedFile:        mapedFile,
		mutex:            new(sync.Mutex),
	}
}

func (self *SelectMapedBufferResult) Release() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.MapedFile != nil {
		self.MapedFile.release()
		self.MapedFile = nil
	}
}

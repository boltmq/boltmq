package stgstorelog

// SelectMapedBufferResult 查询Pagecache返回结果
// Author zhoufei
// Since 2017/9/6
type SelectMapedBufferResult struct {
	StartOffset      int64
	MappedByteBuffer *MappedByteBuffer
	Size             int32
	MapedFile        *MapedFile
}

func NewSelectMapedBufferResult(startOffset int64, mappedByteBuffer *MappedByteBuffer, size int32, mapedFile *MapedFile) *SelectMapedBufferResult {
	return &SelectMapedBufferResult{
		StartOffset:      startOffset,
		MappedByteBuffer: mappedByteBuffer,
		Size:             size,
		MapedFile:        mapedFile,
	}
}

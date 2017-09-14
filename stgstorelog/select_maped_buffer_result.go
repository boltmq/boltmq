package stgstorelog

import "bytes"

// SelectMapedBufferResult 查询Pagecache返回结果
// Author zhoufei
// Since 2017/9/6
type SelectMapedBufferResult struct {
	StartOffset int64
	ByteBuffer  *bytes.Buffer
	Size        int32
	MapedFile   *MapedFile
}

func NewSelectMapedBufferResult(startOffset int64, byteBuffer *bytes.Buffer, size int32, mapedFile *MapedFile) *SelectMapedBufferResult {
	return &SelectMapedBufferResult{
		StartOffset: startOffset,
		ByteBuffer:  byteBuffer,
		Size:        size,
		MapedFile:   mapedFile,
	}
}

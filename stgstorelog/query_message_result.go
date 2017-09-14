package stgstorelog

// QueryMessageResult 通过Key查询消息，返回结果
// Author zhoufei
// Since 2017/9/6
type QueryMessageResult struct {
	MessageMapedList         []*SelectMapedBufferResult // 多个连续的消息集合
	MessageBufferList        []*MappedByteBuffer        // 用来向Consumer传送消息
	IndexLastUpdateTimestamp int64
	IndexLastUpdatePhyoffset int64
	BufferTotalSize          int32 // ByteBuffer 总字节数
}

func NewQueryMessageResult() *QueryMessageResult {
	return &QueryMessageResult{}
}

func (qmr *QueryMessageResult) AddMessage(mapedBuffer *SelectMapedBufferResult) {
	qmr.MessageMapedList = append(qmr.MessageMapedList, mapedBuffer)
}

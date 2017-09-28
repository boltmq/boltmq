package stgstorelog

import "container/list"

// GetMessageResult 访问消息返回结果
// Author gaoyanlei
// Since 2017/8/17
type GetMessageResult struct {
	// 多个连续的消息集合
	MessageMapedList list.List

	// 用来向Consumer传送消息
	MessageBufferList list.List

	// 枚举变量，取消息结果
	Status GetMessageStatus

	// 当被过滤后，返回下一次开始的Offset
	NextBeginOffset int64

	// 逻辑队列中的最小Offset
	MinOffset int64

	// 逻辑队列中的最大Offset
	MaxOffset int64

	// ByteBuffer 总字节数
	BufferTotalSize int

	// 是否建议从slave拉消息
	SuggestPullingFromSlave bool
}

// getMessageCount 获取message个数
// Author gaoyanlei
// Since 2017/8/17
func (self *GetMessageResult) GetMessageCount() int {
	return self.MessageMapedList.Len()
}

func (self *GetMessageResult) addMessage(mapedBuffer *SelectMapedBufferResult) {
	self.MessageMapedList.PushBack(mapedBuffer)
	self.MessageBufferList.PushBack(mapedBuffer.MappedByteBuffer)
	self.BufferTotalSize += int(mapedBuffer.Size)
}

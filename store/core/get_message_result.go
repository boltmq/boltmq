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
package core

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
func (gmr *GetMessageResult) GetMessageCount() int {
	return gmr.MessageMapedList.Len()
}

func (gmr *GetMessageResult) addMessage(mapedBuffer *SelectMapedBufferResult) {
	gmr.MessageMapedList.PushBack(mapedBuffer)
	gmr.MessageBufferList.PushBack(mapedBuffer.MappedByteBuffer)
	gmr.BufferTotalSize += int(mapedBuffer.Size)
}

func (gmr *GetMessageResult) Release() {
	for element := gmr.MessageMapedList.Front(); element != nil; element = element.Next() {
		selectResult := element.Value.(*SelectMapedBufferResult)
		if selectResult != nil {
			selectResult.Release()
		}
	}
}

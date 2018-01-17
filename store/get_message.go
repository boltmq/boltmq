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
package store

import "container/list"

const (
	// 找到消息
	FOUND GetMessageStatus = iota
	// offset正确，但是过滤后没有匹配的消息
	NO_MATCHED_MESSAGE
	// offset正确，但是物理队列消息正在被删除
	MESSAGE_WAS_REMOVING
	// offset正确，但是从逻辑队列没有找到，可能正在被删除
	OFFSET_FOUND_NULL
	// offset错误，严重溢出
	OFFSET_OVERFLOW_BADLY
	// offset错误，溢出1个
	OFFSET_OVERFLOW_ONE
	// offset错误，太小了
	OFFSET_TOO_SMALL
	// 没有对应的逻辑队列
	NO_MATCHED_LOGIC_QUEUE
	// 队列中一条消息都没有
	NO_MESSAGE_IN_QUEUE
)

// GetMessageStatus 访问消息返回的状态码
// Author gaoyanlei
// Since 2017/8/17
type GetMessageStatus int

func (gms GetMessageStatus) String() string {
	switch gms {
	case FOUND:
		return "FOUND"
	case NO_MATCHED_MESSAGE:
		return "NO_MATCHED_MESSAGE"
	case MESSAGE_WAS_REMOVING:
		return "MESSAGE_WAS_REMOVING"
	case OFFSET_FOUND_NULL:
		return "OFFSET_FOUND_NULL"
	case OFFSET_OVERFLOW_BADLY:
		return "OFFSET_OVERFLOW_BADLY"
	case OFFSET_OVERFLOW_ONE:
		return "OFFSET_OVERFLOW_ONE"
	case OFFSET_TOO_SMALL:
		return "OFFSET_TOO_SMALL"
	case NO_MATCHED_LOGIC_QUEUE:
		return "NO_MATCHED_LOGIC_QUEUE"
	case NO_MESSAGE_IN_QUEUE:
		return "NO_MESSAGE_IN_QUEUE"
	default:
		return ""
	}
}

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

// GetMessageCount 获取message个数
// Author gaoyanlei
// Since 2017/8/17
func (gmr *GetMessageResult) GetMessageCount() int {
	return gmr.MessageMapedList.Len()
}

func (gmr *GetMessageResult) AddMessage(bufferResult BufferResult) {
	gmr.MessageMapedList.PushBack(bufferResult)
	gmr.MessageBufferList.PushBack(bufferResult.Buffer())
	gmr.BufferTotalSize += bufferResult.Size()
}

// Release
func (gmr *GetMessageResult) Release() {
	for element := gmr.MessageMapedList.Front(); element != nil; element = element.Next() {
		selectResult := element.Value.(BufferResult)
		if selectResult != nil {
			selectResult.Release()
		}
	}
}

// QueryMessageResult 通过Key查询消息，返回结果
// Author zhoufei
// Since 2017/9/6
type QueryMessageResult struct {
	MessageMapedList         []BufferResult // 多个连续的消息集合
	MessageBufferList        []ByteBuffer   // 用来向Consumer传送消息
	IndexLastUpdateTimestamp int64
	IndexLastUpdatePhyoffset int64
	BufferTotalSize          int32 // ByteBuffer 总字节数
}

func NewQueryMessageResult() *QueryMessageResult {
	return &QueryMessageResult{}
}

func (qmr *QueryMessageResult) AddMessage(bufferResult BufferResult) {
	qmr.MessageMapedList = append(qmr.MessageMapedList, bufferResult)
}

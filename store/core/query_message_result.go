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

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

// GetMessageStatus 访问消息返回的状态码
// Author gaoyanlei
// Since 2017/8/17
type GetMessageStatus int

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

func (self GetMessageStatus) String() string {
	switch self {
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

// Copyright 2017 tantexian

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

// AppendMessageCallback 写消息回调接口
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/6
type AppendMessageCallback interface {
	// write MapedByteBuffer,and return How many bytes to write
	doAppend(fileFromOffset int64, mappedByteBuffer *MappedByteBuffer, maxBlank int32, msg interface{}) *AppendMessageResult
}

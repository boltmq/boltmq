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

const (
	APPENDMESSAGE_PUT_OK AppendMessageStatus = iota
	END_OF_FILE
	MESSAGE_SIZE_EXCEEDED
	APPENDMESSAGE_UNKNOWN_ERROR
)

// AppendMessageCallback 写消息回调接口
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/6
type AppendMessageCallback interface {
	// write MapedByteBuffer,and return How many bytes to write
	DoAppend(fileFromOffset int64, byteBuffer ByteBuffer, maxBlank int32, msg interface{}) *AppendMessageResult
}

// AppendMessageResult 写入commitlong返回结果集
// Author gaoyanlei
// Since 2017/8/16
type AppendMessageResult struct {
	Status         AppendMessageStatus
	WroteOffset    int64
	WroteBytes     int64
	MsgId          string
	StoreTimestamp int64
	LogicsOffset   int64
}

// AppendMessageStatus 写入commitlog 返回code
// Author gaoyanlei
// Since 2017/8/16
type AppendMessageStatus int

func (status AppendMessageStatus) String() string {
	switch status {
	case APPENDMESSAGE_PUT_OK:
		return "PUT_OK"
	case END_OF_FILE:
		return "END_OF_FILE"
	case MESSAGE_SIZE_EXCEEDED:
		return "MESSAGE_SIZE_EXCEEDED"
	case APPENDMESSAGE_UNKNOWN_ERROR:
		return "UNKNOWN_ERROR"
	default:
		return "Unknow"
	}
}

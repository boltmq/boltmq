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
	PUTMESSAGE_PUT_OK PutMessageStatus = iota
	FLUSH_DISK_TIMEOUT
	FLUSH_SLAVE_TIMEOUT
	SLAVE_NOT_AVAILABLE
	SERVICE_NOT_AVAILABLE
	CREATE_MAPPED_FILE_FAILED
	MESSAGE_ILLEGAL
	PUTMESSAGE_UNKNOWN_ERROR
)

// PutMessageStatus 写入消息过程的返回结果
// Author gaoyanlei
// Since 2017/8/16
type PutMessageStatus int

func (status PutMessageStatus) String() string {
	switch status {
	case PUTMESSAGE_PUT_OK:
		return "PUT_OK"
	case FLUSH_DISK_TIMEOUT:
		return "FLUSH_DISK_TIMEOUT"
	case FLUSH_SLAVE_TIMEOUT:
		return "FLUSH_SLAVE_TIMEOUT"
	case SLAVE_NOT_AVAILABLE:
		return "SLAVE_NOT_AVAILABLE"
	case SERVICE_NOT_AVAILABLE:
		return "SERVICE_NOT_AVAILABLE"
	case CREATE_MAPPED_FILE_FAILED:
		return "CREATE_MAPPED_FILE_FAILED"
	case MESSAGE_ILLEGAL:
		return "MESSAGE_ILLEGAL"
	case PUTMESSAGE_UNKNOWN_ERROR:
		return "UNKNOWN_ERROR"
	default:
		return "Unknow"
	}
}

// PutMessageResult 写入消息返回结果
// Author gaoyanlei
// Since 2017/8/16
type PutMessageResult struct {
	Status PutMessageStatus
	Result *AppendMessageResult
}

func (pms *PutMessageResult) IsOk() bool {
	return pms.Result != nil && pms.Result.Status == APPENDMESSAGE_PUT_OK
}

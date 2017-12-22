// Copyright 2017 zhoufei

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

import "sync"

// SelectMapedBufferResult 查询Pagecache返回结果
// Author zhoufei
// Since 2017/9/6
type SelectMapedBufferResult struct {
	StartOffset      int64
	MappedByteBuffer *MappedByteBuffer
	Size             int32
	MapedFile        *MapedFile
	mutex            *sync.Mutex
}

func newSelectMapedBufferResult(startOffset int64, mappedByteBuffer *MappedByteBuffer, size int32, mapedFile *MapedFile) *SelectMapedBufferResult {
	return &SelectMapedBufferResult{
		StartOffset:      startOffset,
		MappedByteBuffer: mappedByteBuffer,
		Size:             size,
		MapedFile:        mapedFile,
		mutex:            new(sync.Mutex),
	}
}

func (smbr *SelectMapedBufferResult) Release() {
	smbr.mutex.Lock()
	defer smbr.mutex.Unlock()

	if smbr.MapedFile != nil {
		smbr.MapedFile.release()
		smbr.MapedFile = nil
	}
}

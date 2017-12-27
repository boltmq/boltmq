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
package persistent

import (
	"sync"

	"github.com/boltmq/boltmq/store"
)

// mappedBufferResult 查询Pagecache返回结果
// Author zhoufei
// Since 2017/9/6
type mappedBufferResult struct {
	byteBuffer  *mappedByteBuffer
	mfile       *mappedFile
	startOffset int64
	size        int32
	mutex       sync.Mutex
}

func newMappedBufferResult(startOffset int64, byteBuffer *mappedByteBuffer, size int32, mfile *mappedFile) *mappedBufferResult {
	return &mappedBufferResult{
		byteBuffer:  byteBuffer,
		mfile:       mfile,
		startOffset: startOffset,
		size:        size,
	}
}

func (mbr *mappedBufferResult) Release() {
	mbr.mutex.Lock()
	defer mbr.mutex.Unlock()

	if mbr.mfile != nil {
		mbr.mfile.release()
		mbr.mfile = nil
	}
}

func (mbr *mappedBufferResult) Buffer() store.ByteBuffer {
	return mbr.byteBuffer
}

func (mbr *mappedBufferResult) Size() int {
	return int(mbr.size)
}

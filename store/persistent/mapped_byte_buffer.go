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
	"bytes"
	"errors"

	"github.com/boltmq/boltmq/store/persistent/mmap"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/convert"
)

type mappedByteBuffer struct {
	mmapBuf  mmap.MMap
	readPos  int // read at &buf[readPos]
	writePos int // write at &buf[writePos]
	limit    int // mmapBuf's max Size
}

func newMappedByteBuffer(mMap mmap.MMap) *mappedByteBuffer {
	mbb := &mappedByteBuffer{}
	mbb.mmapBuf = mMap
	mbb.readPos = 0
	mbb.writePos = 0
	mbb.limit = cap(mMap)
	return mbb
}

func (mbb *mappedByteBuffer) Bytes() []byte {
	return mbb.mmapBuf[:mbb.writePos]
}

// Write appends the contents of data to the buffer
func (mbb *mappedByteBuffer) Write(data []byte) (n int, err error) {
	if mbb.writePos+len(data) > mbb.limit {
		logger.Errorf("mbb.writePos + len(data)(%v > %v) data: %s.", mbb.writePos+len(data), mbb.limit, string(data))
		return 0, errors.New("mbb.writePos + len(data)")
	}
	for index, value := range data {
		mbb.mmapBuf[mbb.writePos+index] = value
	}
	mbb.writePos += len(data)
	return len(data), nil
}

func (mbb *mappedByteBuffer) WriteInt8(i int8) (err error) {
	toBytes := convert.Int8ToBytes(i)
	_, err = mbb.Write(toBytes)
	return
}

func (mbb *mappedByteBuffer) WriteInt16(i int16) (err error) {
	toBytes := convert.Int16ToBytes(i)
	_, err = mbb.Write(toBytes)
	return
}

func (mbb *mappedByteBuffer) WriteInt32(i int32) (err error) {
	toBytes := convert.Int32ToBytes(i)
	_, err = mbb.Write(toBytes)
	return
}

func (mbb *mappedByteBuffer) WriteInt64(i int64) (err error) {
	toBytes := convert.Int64ToBytes(i)
	_, err = mbb.Write(toBytes)
	return
}

// Read reads the next len(p) bytes from the buffer or until the buffer
// is drained. The return value n is the number of bytes read. If the
// buffer has no data to return, err is io.EOF (unless en(p) is zero);
// otherwise it is nil.
func (mbb *mappedByteBuffer) Read(data []byte) (n int, err error) {
	n = copy(data, mbb.mmapBuf[mbb.readPos:])
	mbb.readPos += n
	return
}

func (mbb *mappedByteBuffer) ReadInt8() (i int8) {
	int8bytes := make([]byte, 1)
	mbb.Read(int8bytes)
	i = convert.BytesToInt8(int8bytes)
	return i
}

func (mbb *mappedByteBuffer) ReadInt16() (i int16) {
	int16bytes := make([]byte, 2)
	mbb.Read(int16bytes)
	i = convert.BytesToInt16(int16bytes)
	return i
}

func (mbb *mappedByteBuffer) ReadInt32() (i int32) {
	int32bytes := make([]byte, 4)
	mbb.Read(int32bytes)
	i = convert.BytesToInt32(int32bytes)
	return i
}

func (mbb *mappedByteBuffer) ReadInt64() (i int64) {
	int64bytes := make([]byte, 8)
	mbb.Read(int64bytes)
	i = convert.BytesToInt64(int64bytes)
	return i
}

// slice 返回当前mappedByteBuffer.byteBuffer中从开始位置到len的分片buffer
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/6
func (mbb *mappedByteBuffer) slice() *bytes.Buffer {
	return bytes.NewBuffer(mbb.mmapBuf[:mbb.readPos])
}

func (mbb *mappedByteBuffer) flush() {
	mbb.mmapBuf.Flush()
}

func (mbb *mappedByteBuffer) unmap() {
	mbb.mmapBuf.Unmap()
}

func (mbb *mappedByteBuffer) flip() {
	mbb.limit = mbb.writePos
	mbb.writePos = 0
}

func (mbb *mappedByteBuffer) setlimit(newlimit int) {
	mbb.limit = newlimit
	if mbb.writePos > mbb.limit {
		mbb.writePos = newlimit
	}
}

func clean(mbb *mappedByteBuffer) {
	if mbb == nil {
		return
	}

	// TODO
	mbb.unmap()
}

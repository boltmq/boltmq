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

import (
	"bytes"
	"errors"

	"github.com/boltmq/boltmq/store/core/mmap"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/convert"
)

type MappedByteBuffer struct {
	MMapBuf  mmap.MMap
	ReadPos  int // read at &buf[ReadPos]
	WritePos int // write at &buf[WritePos]
	Limit    int // MMapBuf's max Size
}

func NewMappedByteBuffer(mMap mmap.MMap) *MappedByteBuffer {
	mappedByteBuffer := &MappedByteBuffer{}
	mappedByteBuffer.MMapBuf = mMap
	mappedByteBuffer.ReadPos = 0
	mappedByteBuffer.WritePos = 0
	mappedByteBuffer.Limit = cap(mMap)
	return mappedByteBuffer
}

func (mbb *MappedByteBuffer) Bytes() []byte { return mbb.MMapBuf[:mbb.WritePos] }

// Write appends the contents of data to the buffer
func (mbb *MappedByteBuffer) Write(data []byte) (n int, err error) {
	if mbb.WritePos+len(data) > mbb.Limit {
		logger.Errorf("mbb.WritePos + len(data)(%v > %v) data: %s", mbb.WritePos+len(data), mbb.Limit, string(data))
		//panic(errors.New("m.WritePos + len(data)"))
		return 0, errors.New("mbb.WritePos + len(data)")
	}
	for index, value := range data {
		mbb.MMapBuf[mbb.WritePos+index] = value
	}
	mbb.WritePos += len(data)
	return len(data), nil
}

func (mbb *MappedByteBuffer) WriteInt8(i int8) (mappedByteBuffer *MappedByteBuffer) {
	toBytes := convert.Int8ToBytes(i)
	mbb.Write(toBytes)
	return mbb
}

func (mbb *MappedByteBuffer) WriteInt16(i int16) (mappedByteBuffer *MappedByteBuffer) {
	toBytes := convert.Int16ToBytes(i)
	mbb.Write(toBytes)
	return mbb
}

func (mbb *MappedByteBuffer) WriteInt32(i int32) (mappedByteBuffer *MappedByteBuffer) {
	toBytes := convert.Int32ToBytes(i)
	mbb.Write(toBytes)
	return mbb
}

func (mbb *MappedByteBuffer) WriteInt64(i int64) (mappedByteBuffer *MappedByteBuffer) {
	toBytes := convert.Int64ToBytes(i)
	mbb.Write(toBytes)
	return mbb
}

// Read reads the next len(p) bytes from the buffer or until the buffer
// is drained. The return value n is the number of bytes read. If the
// buffer has no data to return, err is io.EOF (unless en(p) is zero);
// otherwise it is nil.
func (mbb *MappedByteBuffer) Read(data []byte) (n int, err error) {
	n = copy(data, mbb.MMapBuf[mbb.ReadPos:])
	mbb.ReadPos += n
	return
}

func (mbb *MappedByteBuffer) ReadInt8() (i int8) {
	int8bytes := make([]byte, 1)
	mbb.Read(int8bytes)
	i = convert.BytesToInt8(int8bytes)
	return i
}

func (mbb *MappedByteBuffer) ReadInt16() (i int16) {
	int16bytes := make([]byte, 2)
	mbb.Read(int16bytes)
	i = convert.BytesToInt16(int16bytes)
	return i
}

func (mbb *MappedByteBuffer) ReadInt32() (i int32) {
	int32bytes := make([]byte, 4)
	mbb.Read(int32bytes)
	i = convert.BytesToInt32(int32bytes)
	return i
}

func (mbb *MappedByteBuffer) ReadInt64() (i int64) {
	int64bytes := make([]byte, 8)
	mbb.Read(int64bytes)
	i = convert.BytesToInt64(int64bytes)
	return i
}

// slice 返回当前MappedByteBuffer.byteBuffer中从开始位置到len的分片buffer
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/6
func (mbb *MappedByteBuffer) slice() *bytes.Buffer {
	return bytes.NewBuffer(mbb.MMapBuf[:mbb.ReadPos])
}

func (mbb *MappedByteBuffer) flush() {
	mbb.MMapBuf.Flush()
}

func (mbb *MappedByteBuffer) unmap() {
	mbb.MMapBuf.Unmap()
}

func (mbb *MappedByteBuffer) flip() {
	mbb.Limit = mbb.WritePos
	mbb.WritePos = 0
}

func (mbb *MappedByteBuffer) limit(newLimit int) {
	mbb.Limit = newLimit
	if mbb.WritePos > mbb.Limit {
		mbb.WritePos = newLimit
	}
}

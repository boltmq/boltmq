// Copyright 2017 The Authors. All rights reserved.
// Use of this source code is governed by a Apache 
// license that can be found in the LICENSE file.
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/6
package stgstorelog

import "bytes"
import (
	"git.oschina.net/cloudzone/smartgo/stgstorelog/mmap"
	"errors"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
)

type MappedByteBuffer struct {
	MMapBuf  mmap.MemoryMap
	ReadPos  int // read at &buf[ReadPos]
	WritePos int // write at &buf[WritePos]
	Limit    int // MMapBuf's max Size
}

func NewMappedByteBuffer(mMap mmap.MemoryMap) *MappedByteBuffer {
	mappedByteBuffer := &MappedByteBuffer{}
	mappedByteBuffer.MMapBuf = mMap
	mappedByteBuffer.ReadPos = 0
	mappedByteBuffer.WritePos = 0
	mappedByteBuffer.Limit = cap(mMap)
	return mappedByteBuffer
}

func (m *MappedByteBuffer) Bytes() []byte { return m.MMapBuf[:m.WritePos] }

// Write appends the contents of data to the buffer
func (m *MappedByteBuffer) Write(data []byte) (n int, err error) {
	if m.WritePos+len(data) > m.Limit {
		logger.Error("m.WritePos + len(data)(%v > %v)", m.WritePos+len(data), m.Limit)
		panic(errors.New("m.WritePos + len(data)"))
	}
	for index, value := range data {
		m.MMapBuf[m.WritePos+index] = value
	}
	m.WritePos += len(data)
	return len(data), nil
}

func (self *MappedByteBuffer) WriteInt32(i int32) (mappedByteBuffer *MappedByteBuffer) {
	toBytes := utils.Int32ToBytes(i)
	self.Write(toBytes)
	return self
}

// Read reads the next len(p) bytes from the buffer or until the buffer
// is drained. The return value n is the number of bytes read. If the
// buffer has no data to return, err is io.EOF (unless en(p) is zero);
// otherwise it is nil.
func (m *MappedByteBuffer) Read(data []byte) (n int, err error) {
	n = copy(data, m.MMapBuf[m.ReadPos:])
	m.ReadPos += n
	return
}

func (self *MappedByteBuffer) ReadInt32() (i int32) {
	int32bytes := make([]byte, 4)
	self.Read(int32bytes)
	i = utils.BytesToInt32(int32bytes)
	return i
}

// slice 返回当前MappedByteBuffer.byteBuffer中从开始位置到len的分片buffer
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/6
func (self *MappedByteBuffer) slice() (*bytes.Buffer) {
	return bytes.NewBuffer(self.MMapBuf[:self.ReadPos])
}

func (self *MappedByteBuffer) Flush() {
	self.MMapBuf.Flush()
}

func (self *MappedByteBuffer) Unmap() {
	self.MMapBuf.Unmap()
}

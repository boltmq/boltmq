// Copyright 2017 The Authors. All rights reserved.
// Use of this source code is governed by a Apache 
// license that can be found in the LICENSE file.
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/6
package stgstorelog

import (
	"testing"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"bytes"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/mmap"
)

type BytesStr struct {
	buf mmap.MemoryMap
}

func NewBytesStr(mMap mmap.MemoryMap) *BytesStr {
	b := &BytesStr{}
	b.buf = mMap
	return b
}

func TestNewMappedByteBuffer(t *testing.T) {
	//mmapBuffer := NewMappedByteBuffer([]byte("hello world"))
	//mmapBuffer.WriteString(" beautiful golang.")
	//
	//logger.Info("len = %v, cap = %v", mmapBuffer.Len(), mmapBuffer.Cap())
	//logger.Info(string(mmapBuffer.mMapBuf))

	bytes := []byte("hello world")
	lenth := len(bytes)
	bytes[lenth] = '1'
	bytes[lenth+1] = '2'
	logger.Info(string(bytes))

	var mMap mmap.MemoryMap
	mMap = append(mMap, '1')
	logger.Info("mybytes = %v(%p)", mMap, mMap)
	logger.Info("&mybytes = %p", &mMap)

	str := NewBytesStr(mMap)
	logger.Info("str.buf = %v(%p)", str.buf, str.buf)

}

func TestByteBuffer(t *testing.T) {
	mybytes := []byte("Hello world, ")
	logger.Info("mybytes: %v", string(mybytes))

	mybytes = append(mybytes, "the beautiful golang. "...)
	logger.Info("mybytes: %v", string(mybytes))

	buffer := bytes.NewBuffer(mybytes)
	logger.Info("buffer: %v", string(buffer.Bytes()))
	logger.Info("Cap: %v Len: %v", buffer.Cap(), buffer.Len())

	buffer.Write([]byte("bytes buffer is better than []byte."))
	logger.Info("buffer: %v", string(buffer.Bytes()))
	// 读取直到分解符‘g’
	line, _ := buffer.ReadString('g')
	logger.Info("line: %v", line)

	logger.Info("buffer: %v", string(buffer.Bytes()))

	logger.Info("Cap: %v Len: %v", buffer.Cap(), buffer.Len())

	line1, _ := buffer.ReadString('\n')
	logger.Info("line1: %v", line1)
}

func TestByteBufferCapLen(t *testing.T) {
	var buffer bytes.Buffer
	logger.Info("buffer: %v", string(buffer.Bytes()))
	logger.Info("Cap: %v Len: %v", buffer.Cap(), buffer.Len())

	buffer.Write([]byte("hello"))
	logger.Info("buffer: %v", string(buffer.Bytes()))
	logger.Info("Cap: %v Len: %v", buffer.Cap(), buffer.Len())

	buffer.Write([]byte("world"))
	logger.Info("buffer: %v", string(buffer.Bytes()))
	logger.Info("Cap: %v Len: %v", buffer.Cap(), buffer.Len())

	mybytes := make([]byte, 5)
	buffer.Read(mybytes)
	logger.Info("mybytes: %v", string(mybytes))
	logger.Info("buffer: %v", string(buffer.Bytes()))
	logger.Info("Cap: %v Len: %v", buffer.Cap(), buffer.Len())
}

func TestByteBufferReadFrom(t *testing.T) {
	var buffer bytes.Buffer
	logger.Info("buffer: %v", string(buffer.Bytes()))
	logger.Info("Cap: %v Len: %v", buffer.Cap(), buffer.Len())

	buffer.Write([]byte("hello"))
	logger.Info("buffer: %v", string(buffer.Bytes()))
	logger.Info("Cap: %v Len: %v", buffer.Cap(), buffer.Len())

	buffer.Write([]byte("world"))
	logger.Info("buffer: %v", string(buffer.Bytes()))
	logger.Info("Cap: %v Len: %v", buffer.Cap(), buffer.Len())
	var b bytes.Buffer
	b.ReadFrom(&buffer)
	logger.Info("b: %v", string(b.Bytes()))
	logger.Info("buffer: %v", string(buffer.Bytes()))
	logger.Info("Cap: %v Len: %v", buffer.Cap(), buffer.Len())
}

func TestByteBufferT(t *testing.T) {
	var buffer bytes.Buffer
	logger.Info("buffer: %v", string(buffer.Bytes()))
	logger.Info("Cap: %v Len: %v", buffer.Cap(), buffer.Len())

	buffer.Write([]byte("hello"))
	logger.Info("buffer: %v", string(buffer.Bytes()))
	logger.Info("Cap: %v Len: %v", buffer.Cap(), buffer.Len())

	buffer.Write([]byte("world"))
	logger.Info("buffer: %v", string(buffer.Bytes()))
	logger.Info("Cap: %v Len: %v", buffer.Cap(), buffer.Len())

	buffer.Truncate(5)
	logger.Info("buffer: %v", string(buffer.Bytes()))
	logger.Info("Cap: %v Len: %v", buffer.Cap(), buffer.Len())

	buffer.Truncate(2)
	logger.Info("buffer: %v", string(buffer.Bytes()))
	logger.Info("Cap: %v Len: %v", buffer.Cap(), buffer.Len())
}

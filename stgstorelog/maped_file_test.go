// Copyright 2017 The Authors. All rights reserved.
// Use of this source code is governed by a Apache 
// license that can be found in the LICENSE file.
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/6
package stgstorelog

import (
	"testing"
	"os"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"strconv"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/mmap"
)

func TestOpenMapedFile(t *testing.T) {
	mapFile := OpenMapedFile("./unit_test_store/MapedFileTest/", "001")
	maps := mapFile.mappedByteBuffer.MMapBuf
	logger.Info("len == %v cap == %v  \nbuf == %v", len(maps), cap(maps), maps)
}

func TestMapedFile_Write(t *testing.T) {
	mapFile := NewMapedFile("./unit_test_store/MapedFileTest/", "001", 1024*64)
	logger.Info("MMapBuf == %p", mapFile.mappedByteBuffer.MMapBuf)
	msg := "hello word "
	i := 0
	size := 0
	for i <= 64 {
		i++
		data := msg + strconv.Itoa(i)
		size += len(data) * 8
		mapFile.appendMessage([]byte(data))
		// mapFile.MappedByteBuffer.Write([]byte(data))
	}
	logger.Info("data Size == %vk", size/1024)
	//logger.Info(string(mapFile.MappedByteBuffer.MMapBuf))
	mapFile.mappedByteBuffer.Flush()
	mapFile.mappedByteBuffer.Unmap()
}

func TestMapedFile_MMapBufferWithInt32(t *testing.T) {
	mapFile := NewMapedFile("./unit_test_store/MapedFileTest/", "001", 1024*64)
	byteBuffer := mapFile.mappedByteBuffer
	buffer := byteBuffer
	logger.Info("MMapBuf == %p", buffer.MMapBuf)
	i := 0
	size := 0
	for i <= 1024 {
		i++
		size += 4 * 8
		byteBuffer.WriteInt32(int32(i))
	}
	logger.Info("%v", byteBuffer.ReadInt32())
	logger.Info("%v", byteBuffer.ReadInt32())
	logger.Info("%v", byteBuffer.ReadInt32())
	logger.Info("%v", byteBuffer.ReadInt32())
	//logger.Info("MMapBuf == %v Size == %v", buffer.MMapBuf, size)
	buffer.Flush()
	buffer.Unmap()
}

func TestMapedFile_WriteAndRead(t *testing.T) {
	TestMapedFile_MMapBufferWithInt32(t)
	mapFile := OpenMapedFile("./unit_test_store/MapedFileTest/", "001")
	mappedByteBuffer := mapFile.mappedByteBuffer
	logger.Info("-- %v", mappedByteBuffer.ReadInt32())
	logger.Info("-- %v", mappedByteBuffer.ReadInt32())
	logger.Info("-- %v", mappedByteBuffer.ReadInt32())

	// 由于每个int32为4个字节，因此获取数字101，应该为100*4的offset
	mappedByteBuffer.ReadPos = 100 * 4
	logger.Info("-- %v", mappedByteBuffer.ReadInt32())
}

func TestMapedFile_AppendMessageWithCallBack(t *testing.T) {
	// mapFile := NewMapedFile("./unit_test_store/MapedFileTest/", "001", 1024*64)
	//mapFile.AppendMessageWithCallBack()
}

func TestCreateAndRemoveDir(t *testing.T) {
	var mapFile MapedFile
	path := "./tmp/test/"
	mapFile.dirNotExistAndCreateDir(path)
	os.RemoveAll("./tmp")
}

type myAppendMessageWithCallBack struct {
}

func (self *myAppendMessageWithCallBack) doAppend(fileFromOffset int64, mmapBytes mmap.MemoryMap, maxBlank int, msg interface{}) (int) {
	// write MapedByteBuffer,and return How many bytes to write
	return 0
}

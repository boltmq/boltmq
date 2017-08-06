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

type myAppendMessageWithCallBack struct {
}

func (self *myAppendMessageWithCallBack) doAppend(fileFromOffset int64, mmapBytes mmap.MemoryMap, maxBlank int, msg interface{}) (int) {
	// write MapedByteBuffer,and return How many bytes to write
	//msgBytes := []byte(msg)
	//timeBytes := []byte(time.Now().UnixNano())
	//appendBytes := append(msgBytes, timeBytes...)
	//var buffer bytes.Buffer
	////buffer.

	/*currPos := int(self.wrotePostion)
	if currPos+len(data) <= self.fileSize {
		for index, value := range data {
			self.mmapBytes[currPos+index] = value
		}
		atomic.AddInt32(&self.wrotePostion, int32(len(data)))

	return len(appendBytes)*/
	return 0
}

func TestMapedFile_NewMapedFile(t *testing.T) {
	mapFile := NewMapedFile("./unit_test_store/MapedFileTest/", "001", 1024*64)
	logger.Info("mMapBuf == %p", mapFile.mappedByteBuffer.mMapBuf)
	msg := "hello word "
	i := 0
	for i <= 3 {
		i++
		data := msg + strconv.Itoa(i)
		mapFile.appendMessage([]byte(data))
	}
	logger.Info(string(mapFile.mappedByteBuffer.mMapBuf))
	logger.Info("mMapBuf == %p", mapFile.mappedByteBuffer.mMapBuf)
	mapFile.mappedByteBuffer.Flush()
	mapFile.mappedByteBuffer.Unmap()
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

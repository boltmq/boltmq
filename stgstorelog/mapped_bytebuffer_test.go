// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/6
package stgstorelog

import (
	"testing"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"bytes"
)

func TestNewMappedByteBuffer(t *testing.T) {
	myBytes := make([]byte, 100)
	logger.Info("myBytes len == %v, cap == %v", len(myBytes), cap(myBytes))
	buffer := NewMappedByteBuffer(myBytes)
	buffer.WriteInt32(1)
	buffer.WriteInt32(20080808)
	buffer.WriteInt32(9)

	buffer.Write([]byte("Hello"))

	logger.Info("%d", buffer.ReadInt32())
	logger.Info("%d", buffer.ReadInt32())
	logger.Info("%d", buffer.ReadInt32())
	data := make([]byte, 5)
	buffer.Read(data)
	logger.Info(string(data))

	logger.Info(string(buffer.MMapBuf))
}

func TestBytesAndInt32(t *testing.T) {
	int10 := 1288
	toBytes := utils.Int32ToBytes(int32(int10))
	logger.Info("toBytes == %b len == %d ", toBytes, len(toBytes))

	resultInt := utils.BytesToInt32(toBytes)
	logger.Info("resultInt == %d ", resultInt)
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

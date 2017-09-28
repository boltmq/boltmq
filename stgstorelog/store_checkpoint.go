package stgstorelog

import (
	"io/ioutil"
	"os"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/mmap"
	"math"
)

type StoreCheckpoint struct {
	file               *os.File
	mappedByteBuffer   *MappedByteBuffer
	physicMsgTimestamp int64
	logicsMsgTimestamp int64
	indexMsgTimestamp  int64
}

func NewStoreCheckpoint(scpPath string) (*StoreCheckpoint, error) {
	scp := new(StoreCheckpoint)

	scpPathDir := GetParentDirectory(scpPath)
	ensureDirOK(scpPathDir)

	exist, err := PathExists(scpPath)
	if err != nil {
		return nil, err
	}

	if !exist {
		bytes := make([]byte, OS_PAGE_SIZE)
		ioutil.WriteFile(scpPath, bytes, 0666)
	}

	scpFile, err := os.OpenFile(scpPath, os.O_RDWR, 0666)
	scp.file = scpFile
	defer scpFile.Close()
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	mmapBytes, err := mmap.MapRegion(scp.file, MMAPED_ENTIRE_FILE, mmap.RDWR, 0, 0)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	scp.mappedByteBuffer = NewMappedByteBuffer(mmapBytes)

	if exist {
		scp.physicMsgTimestamp = scp.mappedByteBuffer.ReadInt64()
		scp.logicsMsgTimestamp = scp.mappedByteBuffer.ReadInt64()
		scp.indexMsgTimestamp = scp.mappedByteBuffer.ReadInt64()
	}

	return scp, nil
}

func (self *StoreCheckpoint) shutdown() {
	self.flush()
	self.mappedByteBuffer.unmap()
}

func (self *StoreCheckpoint) flush() {
	self.mappedByteBuffer.WritePos = 0
	self.mappedByteBuffer.WriteInt64(self.physicMsgTimestamp)
	self.mappedByteBuffer.WriteInt64(self.logicsMsgTimestamp)
	self.mappedByteBuffer.WriteInt64(self.indexMsgTimestamp)
	self.mappedByteBuffer.flush()
}

func (self *StoreCheckpoint) getMinTimestampIndex() int64 {
	result := math.Min(float64(self.getMinTimestamp()), float64(self.indexMsgTimestamp))
	return int64(result)
}

func (self *StoreCheckpoint) getMinTimestamp() int64 {
	min := math.Min(float64(self.physicMsgTimestamp), float64(self.logicsMsgTimestamp))

	min -= 1000 * 3
	if min < 0 {
		min = 0
	}

	return int64(min)
}

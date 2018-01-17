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
	"io/ioutil"
	"math"
	"os"

	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/boltmq/store/persistent/mmap"
	"github.com/boltmq/common/logger"
)

type storeCheckpoint struct {
	file               *os.File
	byteBuffer         *mappedByteBuffer
	physicMsgTimestamp int64
	logicsMsgTimestamp int64
	indexMsgTimestamp  int64
}

func newStoreCheckpoint(scpPath string) (*storeCheckpoint, error) {
	scp := new(storeCheckpoint)

	scpPathDir := common.ParentDirectory(scpPath)
	common.EnsureDir(scpPathDir)

	exist, err := common.PathExists(scpPath)
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
		logger.Errorf("check point create file err: %s.", err)
		return nil, err
	}

	mmapBytes, err := mmap.MapRegion(scp.file, MMAPED_ENTIRE_FILE, mmap.RDWR, 0, 0)
	if err != nil {
		logger.Errorf("check point mmap err: %s.", err)
		return nil, err
	}

	scp.byteBuffer = newMappedByteBuffer(mmapBytes)

	if exist {
		scp.physicMsgTimestamp = scp.byteBuffer.ReadInt64()
		scp.logicsMsgTimestamp = scp.byteBuffer.ReadInt64()
		scp.indexMsgTimestamp = scp.byteBuffer.ReadInt64()
	}

	return scp, nil
}

func (scp *storeCheckpoint) shutdown() {
	scp.flush()
	scp.byteBuffer.unmap()
}

func (scp *storeCheckpoint) flush() {
	scp.byteBuffer.writePos = 0
	scp.byteBuffer.WriteInt64(scp.physicMsgTimestamp)
	scp.byteBuffer.WriteInt64(scp.logicsMsgTimestamp)
	scp.byteBuffer.WriteInt64(scp.indexMsgTimestamp)
	scp.byteBuffer.flush()
}

func (scp *storeCheckpoint) getMinTimestampIndex() int64 {
	result := math.Min(float64(scp.getMinTimestamp()), float64(scp.indexMsgTimestamp))
	return int64(result)
}

func (scp *storeCheckpoint) getMinTimestamp() int64 {
	min := math.Min(float64(scp.physicMsgTimestamp), float64(scp.logicsMsgTimestamp))

	min -= 1000 * 3
	if min < 0 {
		min = 0
	}

	return int64(min)
}

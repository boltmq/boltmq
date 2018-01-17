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
	"time"

	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/system"
	concurrent "github.com/fanliao/go-concurrentMap"
)

const (
	WaitTimeOut              = 1000 * 5
	DEFAULT_INITIAL_CAPACITY = 11
)

// allocateRequest 分配请求
type allocateRequest struct {
	filePath string
	fileSize int64
	syncChan chan bool
	mf       *mappedFile
}

func newAllocateRequest(filePath string, fileSize int64) *allocateRequest {
	request := new(allocateRequest)
	request.filePath = filePath
	request.fileSize = fileSize
	request.syncChan = make(chan bool, 1)

	return request
}

func (areq *allocateRequest) compareTo(request *allocateRequest) int {
	if areq.fileSize < request.fileSize {
		return 1
	}

	if areq.fileSize > request.fileSize {
		return -1
	}

	return 0
}

type allocateMappedFileService struct {
	requestTable *concurrent.ConcurrentMap
	requestChan  chan *allocateRequest
	hasException bool
}

func newAllocateMappedFileService() *allocateMappedFileService {
	ams := new(allocateMappedFileService)
	ams.requestTable = concurrent.NewConcurrentMap()
	ams.requestChan = make(chan *allocateRequest, DEFAULT_INITIAL_CAPACITY)
	return ams
}

func (amfs *allocateMappedFileService) putRequestAndReturnMappedFile(nextFilePath string, nextNextFilePath string, filesize int64) (*mappedFile, error) {
	nextReq := newAllocateRequest(nextFilePath, filesize)
	nextNextReq := newAllocateRequest(nextNextFilePath, filesize)

	oldValue, err := amfs.requestTable.PutIfAbsent(nextFilePath, nextReq)
	if err != nil {
		logger.Errorf("allocate mapped file service put request err: %s.", err)
		return nil, nil
	}

	if oldValue == nil {
		amfs.requestChan <- nextReq
	}

	nextOldValue, err := amfs.requestTable.PutIfAbsent(nextNextFilePath, nextNextReq)
	if err != nil {
		logger.Errorf("allocate mapped file service put request err: %s.", err)
		return nil, nil
	}

	if nextOldValue == nil {
		amfs.requestChan <- nextNextReq
	}

	result, err := amfs.requestTable.Get(nextFilePath)
	if err != nil {
		logger.Errorf("allocate mapped file service get request by file path err: %s.", err)
	}

	if result != nil {
		request := result.(*allocateRequest)
		select {
		case <-request.syncChan:
			logger.Info("allocate mmap file sync chan complete.")
			break
		case <-time.After(WaitTimeOut * time.Millisecond):
			logger.Warnf("create mmap timeout %s %d.", request.filePath, request.fileSize)
		}

		amfs.requestTable.Remove(nextFilePath)

		return request.mf, nil
	} else {
		logger.Error("find preallocate mmap failed, this never happen.")
	}

	return nil, nil
}

func (amfs *allocateMappedFileService) mmapOperation() bool {
	select {
	case request := <-amfs.requestChan:
		value, err := amfs.requestTable.Get(request.filePath)
		if err != nil {
			logger.Errorf("allocate mapped file service mmapOperation get request err: %s.", err)
			return true
		}

		if value == nil {
			logger.Warnf("this mmap request expired, maybe cause timeout %s %s.", request.filePath, request.fileSize)
			return true
		}

		if request.mf == nil {
			beginTime := system.CurrentTimeMillis()
			mf, err := newMappedFile(request.filePath, request.fileSize)
			if mf == nil {
				logger.Error("allocate service new mapped file failed.")
			}

			if err != nil {
				logger.Warnf("allocate mapped file service has exception, maybe by shutdown. err: %s.", err)
				return false
			}

			eclipseTime := beginTime - system.CurrentTimeMillis()
			if eclipseTime > 10 {
				// TODO
			}

			request.mf = mf
		}

		request.syncChan <- true
		break
	}

	return true
}

func (amfs *allocateMappedFileService) start() {
	logger.Info("allocate mapped file service started.")
	for {
		if amfs.mmapOperation() {

		}
	}
}

func (amfs *allocateMappedFileService) shutdown() {
	for iterator := amfs.requestTable.Iterator(); iterator.HasNext(); {
		_, value, ok := iterator.Next()
		if ok {
			request := value.(*allocateRequest)
			logger.Infof("delete pre allocated mapped file, %s.", request.mf.fileName)
			success := request.mf.destroy(1000)
			if !success {
				time.Sleep(time.Millisecond * 1)
				for i := 0; i < 3; i++ {
					if request.mf.destroy(1000) {
						break
					}
				}
			}
		}
	}

}

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
package store

import (
	"time"

	"github.com/boltmq/boltmq/store/core"
	"github.com/boltmq/common/logger"
	concurrent "github.com/fanliao/go-concurrentMap"
)

const (
	WaitTimeOut              = 1000 * 5
	DEFAULT_INITIAL_CAPACITY = 11
)

// AllocateRequest 分配请求
type AllocateRequest struct {
	filePath  string
	fileSize  int64
	syncChan  chan bool
	mapedFile *core.MapedFile
}

func NewAllocateRequest(filePath string, fileSize int64) *AllocateRequest {
	request := new(AllocateRequest)
	request.filePath = filePath
	request.fileSize = fileSize
	request.syncChan = make(chan bool, 1)

	return request
}

func (areq *AllocateRequest) compareTo(request *AllocateRequest) int {
	if areq.fileSize < request.fileSize {
		return 1
	}

	if areq.fileSize > request.fileSize {
		return -1
	}

	return 0
}

// AllocateMapedFileService 预分配maped文件
type AllocateMapedFileService struct {
	requestTable *concurrent.ConcurrentMap
	requestChan  chan *AllocateRequest
	hasException bool
}

func NewAllocateMapedFileService() *AllocateMapedFileService {
	ams := new(AllocateMapedFileService)
	ams.requestTable = concurrent.NewConcurrentMap()
	ams.requestChan = make(chan *AllocateRequest, DEFAULT_INITIAL_CAPACITY)
	return ams
}

func (amfs *AllocateMapedFileService) PutRequestAndReturnMapedFile(nextFilePath string, nextNextFilePath string, filesize int64) (*core.MapedFile, error) {
	nextReq := NewAllocateRequest(nextFilePath, filesize)
	nextNextReq := NewAllocateRequest(nextNextFilePath, filesize)

	oldValue, err := amfs.requestTable.PutIfAbsent(nextFilePath, nextReq)
	if err != nil {
		logger.Info("allocate maped file service put request error:", err.Error())
		return nil, nil
	}

	if oldValue == nil {
		amfs.requestChan <- nextReq
	}

	nextOldValue, err := amfs.requestTable.PutIfAbsent(nextNextFilePath, nextNextReq)
	if err != nil {
		logger.Info("allocate maped file service put request error:", err.Error())
		return nil, nil
	}

	if nextOldValue == nil {
		amfs.requestChan <- nextNextReq
	}

	result, err := amfs.requestTable.Get(nextFilePath)
	if err != nil {
		logger.Info("allocate maped file service get request by file path error:", err.Error())
	}

	if result != nil {
		request := result.(*AllocateRequest)

		select {
		case <-request.syncChan:
			logger.Info("sync chan complete")
			break
		case <-time.After(WaitTimeOut * time.Millisecond):
			logger.Warnf("create mmap timeout %s %d", request.filePath, request.fileSize)
		}

		amfs.requestTable.Remove(nextFilePath)

		return request.mapedFile, nil
	} else {
		logger.Error("find preallocate mmap failed, this never happen")
	}

	return nil, nil
}

func (amfs *AllocateMapedFileService) mmapOperation() bool {
	select {
	case request := <-amfs.requestChan:
		value, err := amfs.requestTable.Get(request.filePath)
		if err != nil {
			logger.Info("allocate maped file service mmapOperation get request error:", err.Error())
			return true
		}

		if value == nil {
			logger.Warnf("this mmap request expired, maybe cause timeout %s %s", request.filePath, request.fileSize)
			return true
		}

		if request.mapedFile == nil {
			beginTime := time.Now().UnixNano() / 1000000
			mapedFile, err := core.NewMapedFile(request.filePath, request.fileSize)
			if mapedFile == nil {
				logger.Error("New Maped File")
			}

			if err != nil {
				logger.Warn("allocate maped file service has exception, maybe by shutdown,error:", err.Error())
				return false
			}

			eclipseTime := beginTime - time.Now().UnixNano()/1000000
			if eclipseTime > 10 {
				// TODO
			}

			request.mapedFile = mapedFile
		}

		request.syncChan <- true

		break
	}

	return true
}

func (amfs *AllocateMapedFileService) Start() {
	logger.Info("allocate maped file service started")
	for {
		if amfs.mmapOperation() {

		}
	}
}

func (amfs *AllocateMapedFileService) Shutdown() {
	for iterator := amfs.requestTable.Iterator(); iterator.HasNext(); {
		_, value, ok := iterator.Next()
		if ok {
			request := value.(*AllocateRequest)
			logger.Info("delete pre allocated maped file, ", request.mapedFile.FileName())
			success := request.mapedFile.Destroy(1000)
			if !success {
				time.Sleep(time.Millisecond * 1)
				for i := 0; i < 3; i++ {
					if request.mapedFile.Destroy(1000) {
						break
					}
				}
			}
		}
	}

}

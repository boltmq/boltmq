// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/7
package stgstorelog

import (
	"time"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"github.com/fanliao/go-concurrentMap"
)

const (
	WaitTimeOut              = 1000 * 5
	DEFAULT_INITIAL_CAPACITY = 11
)

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

func (self *AllocateMapedFileService) putRequestAndReturnMapedFile(nextFilePath string, nextNextFilePath string, filesize int64) (*MapedFile, error) {
	nextReq := NewAllocateRequest(nextFilePath, filesize)
	nextNextReq := NewAllocateRequest(nextNextFilePath, filesize)

	oldValue, err := self.requestTable.PutIfAbsent(nextFilePath, nextReq)
	if err != nil {
		logger.Info(err.Error())
		return nil, nil
	}

	if oldValue == nil {
		self.requestChan <- nextReq
	}

	nextOldValue, err := self.requestTable.PutIfAbsent(nextNextFilePath, nextNextReq)
	if err != nil {
		logger.Info(err.Error())
		return nil, nil
	}

	if nextOldValue == nil {
		self.requestChan <- nextNextReq
	}

	result, err := self.requestTable.Get(nextFilePath)
	if err != nil {
		logger.Info(err.Error())
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

		self.requestTable.Remove(nextFilePath)

		return request.mapedFile, nil
	} else {
		logger.Error("find preallocate mmap failed, this never happen")
	}

	return nil, nil
}

func (self *AllocateMapedFileService) mmapOperation() bool {
	select {
	case request := <-self.requestChan:
		value, err := self.requestTable.Get(request.filePath)
		if err != nil {
			logger.Info(err.Error())
			return true
		}

		if value == nil {
			logger.Warnf("this mmap request expired, maybe cause timeout %s %s", request.filePath, request.fileSize)
			return true
		}

		if request.mapedFile == nil {
			beginTime := time.Now().UnixNano() / 1000000
			mapedFile, err := NewMapedFile(request.filePath, request.fileSize)
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

func (self *AllocateMapedFileService) Start() {
	logger.Info("allocate maped file service started")
	for {
		if self.mmapOperation() {

		}
	}
}

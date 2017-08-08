// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/7
package stgstorelog

import (
	"container/list"
	"sync"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"path/filepath"
	"os"
)

type MapedFileQueue struct {
	// 每次触发删除文件，最多删除多少个文件
	DeleteFilesBatchMax int
	// 文件存储位置
	storePath string
	// 每个文件的大小
	mapedFileSize int
	// 各个文件
	mapedFiles list.List
	// 读写锁（针对mapedFiles）
	rwLock *sync.RWMutex
	// 预分配MapedFile对象服务
	allocateMapedFileService AllocateMapedFileService
	// 刷盘刷到哪里
	committedWhere int64
	// 最后一条消息存储时间
	storeTimestamp int64
}

func NewMapedFileQueue(storePath string, mapedFileSize int, allocateMapedFileService AllocateMapedFileService) *MapedFileQueue {
	self := &MapedFileQueue{}
	self.storePath = storePath;         // 存储路径
	self.mapedFileSize = mapedFileSize; // 文件size
	// 根据读写请求队列requestQueue/readQueue中的读写请求，创建对应的mappedFile文件
	self.allocateMapedFileService = allocateMapedFileService
	self.DeleteFilesBatchMax = 10
	self.committedWhere = 0
	self.storeTimestamp = 0
	return self
}

func (self *MapedFileQueue) getMapedFileByTime(timestamp int64) (mf MapedFile) {
	mapedFileSlice := self.copyMapedFiles(0)
	if mapedFileSlice == nil {
		// TODO: return nil
		// mf = nil
		return
	}
	for _, mf := range mapedFileSlice {
		if mf.storeTimestamp >= timestamp {
			return mf
		}
	}
	return mapedFileSlice[len(mapedFileSlice)-1]
}

func (self *MapedFileQueue) copyMapedFiles(reservedMapedFiles int) []MapedFile {
	mapedFileSlice := make([]MapedFile, 10)
	self.rwLock.RLock()
	defer self.rwLock.RUnlock()
	if self.mapedFiles.Len() <= reservedMapedFiles {
		return nil
	}
	// Iterate through list and print its contents.
	for e := self.mapedFiles.Front(); e != nil; e = e.Next() {
		mapedFileSlice = append(mapedFileSlice, e.Value.(MapedFile))
	}
	return mapedFileSlice
}

// truncateDirtyFiles recover时调用，不需要加锁
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/7
func (self *MapedFileQueue) truncateDirtyFiles(offset int64) {
	willRemoveFiles := list.New()
	// Iterate through list and print its contents.
	for e := self.mapedFiles.Front(); e != nil; e = e.Next() {
		mf := e.Value.(MapedFile)
		fileTailOffset := mf.fileFromOffset + mf.fileSize
		if offset >= fileTailOffset {
			pos := int32(offset % int64(self.mapedFileSize))
			mf.wrotePostion = pos
			mf.committedPosition = pos
		} else {
			mf.destroy()
			willRemoveFiles.PushBack(mf)
		}
	}
	self.deleteExpiredFile(willRemoveFiles)
}

// deleteExpiredFile 删除过期文件只能从头开始删
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/7
func (self *MapedFileQueue) deleteExpiredFile(mfs *list.List) {
	if mfs.Len() != 0 {
		self.rwLock.Lock()
		defer self.rwLock.Unlock()
		for e := mfs.Front(); e != nil; e = e.Next() {
			success := self.mapedFiles.Remove(e)
			if success == false {
				logger.Error("deleteExpiredFile remove failed.")
			}
		}
	}
}

func (self *MapedFileQueue) load() bool {
	filepath.Walk(self.storePath, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			logger.Info("dir : %v", path)
		}
		logger.Info("file : %v", path)
		return nil
	})
	return true
}

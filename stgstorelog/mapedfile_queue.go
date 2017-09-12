// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/7
package stgstorelog

import (
	"container/list"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/fileutil"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
)

type MapedFileQueue struct {
	// 每次触发删除文件，最多删除多少个文件
	DeleteFilesBatchMax int
	// 文件存储位置
	storePath string
	// 每个文件的大小
	mapedFileSize int64
	// 各个文件
	mapedFiles list.List
	// 读写锁（针对mapedFiles）
	rwLock sync.RWMutex
	// 预分配MapedFile对象服务
	allocateMapedFileService *AllocateMapedFileService
	// 刷盘刷到哪里
	committedWhere int64
	// 最后一条消息存储时间
	storeTimestamp int64
}

func NewMapedFileQueue(storePath string, mapedFileSize int64,
	allocateMapedFileService *AllocateMapedFileService) *MapedFileQueue {
	self := &MapedFileQueue{}
	self.storePath = storePath         // 存储路径
	self.mapedFileSize = mapedFileSize // 文件size
	// 根据读写请求队列requestQueue/readQueue中的读写请求，创建对应的mappedFile文件
	self.allocateMapedFileService = allocateMapedFileService
	self.DeleteFilesBatchMax = 10
	self.committedWhere = 0
	self.storeTimestamp = 0
	return self
}

func (self *MapedFileQueue) getMapedFileByTime(timestamp int64) (mf *MapedFile) {
	mapedFileSlice := self.copyMapedFiles(0)
	if mapedFileSlice == nil {
		return nil
	}
	for _, mf := range mapedFileSlice {
		if mf.storeTimestamp >= timestamp {
			return &mf
		}
	}
	return &mapedFileSlice[len(mapedFileSlice)-1]
}

// copyMapedFiles 获取当前mapedFiles列表中的副本slice
// Params: reservedMapedFiles 只有当mapedFiles列表中文件个数大于reservedMapedFiles才返回副本
// Return: 返回大于等于reservedMapedFiles个元数的MapedFile切片
// Author: tantexian, <tantexian@qq.com>
// Since: 17/8/9
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
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/7
func (self *MapedFileQueue) truncateDirtyFiles(offset int64) {
	willRemoveFiles := list.New()
	// Iterate through list and print its contents.
	for e := self.mapedFiles.Front(); e != nil; e = e.Next() {
		mf := e.Value.(MapedFile)
		fileTailOffset := mf.fileFromOffset + mf.fileSize
		if offset >= fileTailOffset {
			pos := offset % int64(self.mapedFileSize)
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
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/7
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

// load 从磁盘加载mapedfile到内存映射
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/8
func (self *MapedFileQueue) load() bool {
	exist, err := PathExists(self.storePath)
	if err != nil {
		logger.Info(err.Error())
		return false
	}

	if exist {
		files, err := fileutil.ListFilesOrDir(self.storePath, "FILE")
		if err != nil {
			logger.Error(err.Error())
			return false
		}
		if len(files) != 0 {
			// 按照文件名，升序排列
			sort.Strings(files)
			for _, path := range files {
				file, error := os.OpenFile(path, os.O_RDONLY, 0666)
				if error != nil {
					logger.Error(error.Error())
				}
				size, error := fileutil.FileSize(file)
				if error != nil {
					logger.Error(error.Error())
				}
				// 校验文件大小是否匹配
				if size != int64(self.mapedFileSize) {
					logger.Warn("filesize(%d) mapedFileSize(%d) length not matched message store config value, ignore it", size, self.mapedFileSize)
					return true
				}

				// 恢复队列
				mapedFile, error := NewMapedFile(self.storePath, int64(self.mapedFileSize))
				if error != nil {
					logger.Error(error.Error())
					return false
				}
				mapedFile.wrotePostion = self.mapedFileSize
				mapedFile.committedPosition = self.mapedFileSize
				self.mapedFiles.PushBack(mapedFile)
				logger.Info("load mapfiled %v success.", mapedFile.fileName)
			}
		}
	}

	return true
}

// howMuchFallBehind 刷盘进度落后了多少
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/8
func (self *MapedFileQueue) howMuchFallBehind() int64 {
	if self.mapedFiles.Len() == 0 {
		return 0
	}
	committed := self.committedWhere
	if committed != 0 {
		mapedFile, error := self.getLastMapedFile(0)
		if error != nil {
			logger.Error(error.Error())
		}
		return mapedFile.fileFromOffset + mapedFile.wrotePostion - committed
	}
	return 0
}

// getLastMapedFile 获取最后一个MapedFile对象，如果一个都没有，则新创建一个，
// 如果最后一个写满了，则新创建一个
// Params: startOffset 如果创建新的文件，起始offset
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/8
func (self *MapedFileQueue) getLastMapedFile(startOffset int64) (*MapedFile, error) {
	var createOffset int64 = -1
	var mapedFile, mapedFileLast *MapedFile
	self.rwLock.RLock()
	if self.mapedFiles.Len() == 0 {
		// 此处createOffset即后续步骤中用来为文件命令用
		createOffset = startOffset - (startOffset % int64(self.mapedFileSize))
	} else {
		// 如果mapedFile不为空，则获取list的最后一个文件
		mapedFileLastObj := (self.mapedFiles.Back().Value).(*MapedFile)
		mapedFileLast = mapedFileLastObj
	}
	self.rwLock.RUnlock()
	if mapedFileLast != nil && mapedFileLast.isFull() {
		createOffset = mapedFileLast.fileFromOffset + self.mapedFileSize

	}

	if createOffset != -1 {
		nextPath := self.storePath + string(filepath.Separator) + fileutil.Offset2FileName(createOffset)
		nextNextPath := self.storePath + string(filepath.Separator) +
			fileutil.Offset2FileName(createOffset+self.mapedFileSize)
		if self.allocateMapedFileService != nil {
			var err error
			mapedFile, err = self.allocateMapedFileService.putRequestAndReturnMapedFile(nextPath, nextNextPath, self.mapedFileSize)
			if err != nil {
				return nil, err
			}
		} else {
			var err error
			mapedFile, err = NewMapedFile(nextPath, self.mapedFileSize)
			if err != nil {
				return nil, err
			}
		}

		self.rwLock.Lock()
		if self.mapedFiles.Len() == 0 {
			mapedFile.firstCreateInQueue = true
		}
		self.mapedFiles.PushBack(mapedFile)
		self.rwLock.Unlock()
		return mapedFile, nil
	}

	return mapedFileLast, nil
}

func (self *MapedFileQueue) getMinOffset() int64 {
	self.rwLock.RLock()
	defer self.rwLock.RUnlock()
	if self.mapedFiles.Len() > 0 {
		mappedfile := self.mapedFiles.Front().Value.(MapedFile)
		return mappedfile.fileFromOffset
	}

	return -1
}

func (self *MapedFileQueue) getMaxOffset() int64 {
	self.rwLock.RLock()
	defer self.rwLock.RUnlock()
	if self.mapedFiles.Len() > 0 {
		mappedfile := self.mapedFiles.Back().Value.(MapedFile)
		return mappedfile.fileFromOffset
	}

	return 0
}

// deleteLastMapedFile 恢复时调用
func (self *MapedFileQueue) deleteLastMapedFile() {
	if self.mapedFiles.Len() != 0 {
		last := self.mapedFiles.Back()
		lastMapedFile := last.Value.(MapedFile)
		lastMapedFile.destroy()
		self.mapedFiles.Remove(last)
		logger.Info("on recover, destroy a logic maped file %v", lastMapedFile.fileName)
	}
}

// deleteExpiredFileByTime 根据文件过期时间来删除物理队列文件
// Return: 删除过期文件的数量
// Author: tantexian, <tantexian@qq.com>
// Since: 17/8/9
func (self *MapedFileQueue) deleteExpiredFileByTime(expiredTime int64, deleteFilesInterval int,
	intervalForcibly int64, cleanImmediately bool) int {
	// 获取当前MapedFiles列表中所有元素副本的切片
	files := self.copyMapedFiles(0)
	if len(files) == 0 {
		return 0
	}
	var toBeDeleteMfList list.List
	var delCount int = 0
	// 最后一个文件处于写状态，不能删除
	for index, mf := range files {
		liveMaxTimestamp := mf.storeTimestamp + expiredTime
		if timeutil.NowTimestamp() > liveMaxTimestamp || cleanImmediately {
			// TODO: 此处不存在destroy失败的情况？
			mf.destroy()
			toBeDeleteMfList.PushBack(mf)
			delCount++
			// 每次触发删除文件，最多删除多少个文件
			if toBeDeleteMfList.Len() >= self.DeleteFilesBatchMax {
				break
			}
			// 删除最后一个文件不需要等待
			if deleteFilesInterval > 0 && (index+1) < len(files) {
				time.Sleep(time.Second * time.Duration(deleteFilesInterval))
			}
		}
	}

	self.deleteExpiredFile(&toBeDeleteMfList)

	return delCount
}

// deleteExpiredFileByOffset 根据物理队列最小Offset来删除逻辑队列
// Params: offset 物理队列最小offset
// Params: unitsize ???
// Author: tantexian, <tantexian@qq.com>
// Since: 17/8/9
func (self *MapedFileQueue) deleteExpiredFileByOffset(offset int64, unitsize int) int {
	var toBeDeleteFileList *list.List
	deleteCount := 0
	mfs := self.copyMapedFiles(0)
	// 最后一个文件处于写状态，不能删除
	mfs = mfs[:len(mfs)-2]
	for _, mf := range mfs {
		// TODO: 此处是否可以直接destroy？
		// FIXME: 根据offset删除逻辑队列consumerQueue待实现
		canDestory := false
		toBeDeleteFileList.PushBack(mf)
		mf.destroy()
		// 当前文件是否可以删除
		//canDestory = (maxOffsetInLogicQueue < offset)

		if canDestory {
			toBeDeleteFileList.PushBack(mf)
			deleteCount++
		}
	}
	self.deleteExpiredFile(toBeDeleteFileList)
	return deleteCount
}

func (self *MapedFileQueue) commit(flushLeastPages int) {
	// TODO:
}

func (self *MapedFileQueue) findMapedFileByOffset() *MapedFile {
	return &MapedFile{}
}

func (self *MapedFileQueue) getFirstMapedFile() *MapedFile {
	return &MapedFile{}
}

func (self *MapedFileQueue) getLastAndLastMapedFile() *MapedFile {
	return &MapedFile{}
}

func (self *MapedFileQueue) getMapedMemorySize() int64 {
	return 0
}

func (self *MapedFileQueue) retryDeleteFirstFile(intervalForcibly int64) bool {
	return true
}

func (self *MapedFileQueue) getFirstMapedFileOnLock() *MapedFile {
	return &MapedFile{}
}

// shutdown 关闭队列，队列数据还在，但是不能访问
func (self *MapedFileQueue) shutdown(intervalForcibly int64) {

}

// destroy 销毁队列，队列数据被删除，此函数有可能不成功
func (self *MapedFileQueue) destroy() {

}

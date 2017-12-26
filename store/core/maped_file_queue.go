// Copyright 2017 tantexian

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package core

import (
	"container/list"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/boltmq/common/logger"
)

// allocateMapedFileStrategy 分配maped file方式
type allocateMapedFileStrategy interface {
	PutRequestAndReturnMapedFile(nextFilePath string, nextNextFilePath string, filesize int64) (*MapedFile, error)
}

type MapedFileQueue struct {
	// 每次触发删除文件，最多删除多少个文件
	deleteFilesBatchMax int
	// 文件存储位置
	storePath string
	// 每个文件的大小
	mapedFileSize int64
	// 各个文件
	mapedFiles *list.List
	// 读写锁（针对mapedFiles）
	rwLock sync.RWMutex
	// 预分配MapedFile对象服务
	allocateMFStrategy allocateMapedFileStrategy
	// 刷盘刷到哪里
	committedWhere int64
	// 最后一条消息存储时间
	storeTimestamp int64
}

// NewMapedFileQueue create maped file queue
func NewMapedFileQueue(storePath string, mapedFileSize int64,
	allocateMFStrategy allocateMapedFileStrategy) *MapedFileQueue {
	mfq := &MapedFileQueue{}
	mfq.storePath = storePath         // 存储路径
	mfq.mapedFileSize = mapedFileSize // 文件size
	mfq.mapedFiles = list.New()
	// 根据读写请求队列requestQueue/readQueue中的读写请求，创建对应的mappedFile文件
	mfq.allocateMFStrategy = allocateMFStrategy
	mfq.deleteFilesBatchMax = 10
	mfq.committedWhere = 0
	mfq.storeTimestamp = 0

	return mfq
}

func (mfq *MapedFileQueue) GetMapedFileByTime(timestamp int64) (mf *MapedFile) {
	mapedFileSlice := mfq.copyMapedFiles(0)
	if mapedFileSlice == nil {
		return nil
	}

	for _, mf := range mapedFileSlice {
		if mf != nil {
			fileInfo, err := os.Stat(mf.fileName)
			if err != nil {
				logger.Warn("maped file queue get maped file by time error:", err.Error())
				continue
			}

			modifiedTime := fileInfo.ModTime().UnixNano() / 1000000
			if modifiedTime >= timestamp {
				return mf
			}
		}
	}

	return mapedFileSlice[len(mapedFileSlice)-1]
}

// copyMapedFiles 获取当前mapedFiles列表中的副本slice
// Params: reservedMapedFiles 只有当mapedFiles列表中文件个数大于reservedMapedFiles才返回副本
// Return: 返回大于等于reservedMapedFiles个元数的MapedFile切片
// Author: tantexian, <tantexian@qq.com>
// Since: 17/8/9
func (mfq *MapedFileQueue) copyMapedFiles(reservedMapedFiles int) []*MapedFile {
	mapedFileSlice := make([]*MapedFile, mfq.mapedFiles.Len())
	mfq.rwLock.RLock()
	defer mfq.rwLock.RUnlock()
	if mfq.mapedFiles.Len() <= reservedMapedFiles {
		return nil
	}

	// Iterate through list and print its contents.
	for e := mfq.mapedFiles.Front(); e != nil; e = e.Next() {
		mf := e.Value.(*MapedFile)
		if mf != nil {
			mapedFileSlice = append(mapedFileSlice, mf)
		}
	}

	return mapedFileSlice
}

// TruncateDirtyFiles recover时调用，不需要加锁
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/7
func (mfq *MapedFileQueue) TruncateDirtyFiles(offset int64) {
	willRemoveFiles := list.New()

	// Iterate through list and print its contents.
	for e := mfq.mapedFiles.Front(); e != nil; e = e.Next() {
		mf := e.Value.(*MapedFile)
		fileTailOffset := mf.fileFromOffset + mf.fileSize
		if fileTailOffset > offset {
			if offset >= mf.fileFromOffset {
				pos := offset % int64(mfq.mapedFileSize)
				mf.WrotePostion = pos
				mf.MByteBuffer.WritePos = int(pos)
				mf.CommittedPosition = pos
			} else {
				mf.Destroy(1000)
				willRemoveFiles.PushBack(mf)
			}
		}
	}
	mfq.DeleteExpiredFile(willRemoveFiles)
}

// DeleteExpiredFile 删除过期文件只能从头开始删
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/7
func (mfq *MapedFileQueue) DeleteExpiredFile(mfs *list.List) {
	if mfs != nil && mfs.Len() > 0 {
		mfq.rwLock.Lock()
		defer mfq.rwLock.Unlock()
		for de := mfs.Front(); de != nil; de = de.Next() {
			for e := mfq.mapedFiles.Front(); e != nil; e = e.Next() {
				deleteFile := de.Value.(*MapedFile)
				file := e.Value.(*MapedFile)

				if deleteFile.fileName == file.fileName {
					success := mfq.mapedFiles.Remove(e)
					if success == false {
						logger.Error("deleteExpiredFile remove failed.")
					}
				}
			}
		}
	}
}

// Load 从磁盘加载mapedfile到内存映射
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/8
func (mfq *MapedFileQueue) Load() bool {
	exist, err := PathExists(mfq.storePath)
	if err != nil {
		logger.Infof("maped file queue load store path error:", err.Error())
		return false
	}

	if exist {
		files, err := ListFilesOrDir(mfq.storePath, "FILE")
		if err != nil {
			logger.Error(err.Error())
			return false
		}
		if len(files) > 0 {
			// 按照文件名，升序排列
			sort.Strings(files)
			for _, path := range files {
				file, error := os.Stat(path)
				if error != nil {
					logger.Errorf("maped file queue load file %s error: %s", path, error.Error())
				}

				if file == nil {
					logger.Errorf("maped file queue load file not exist: ", path)
				}

				size := file.Size()
				// 校验文件大小是否匹配
				if size != int64(mfq.mapedFileSize) {
					logger.Warnf("filesize(%d) mapedFileSize(%d) length not matched message store config value, ignore it", size, mfq.mapedFileSize)
					return true
				}

				// 恢复队列
				mapedFile, error := NewMapedFile(path, int64(mfq.mapedFileSize))
				if error != nil {
					logger.Error("maped file queue load file error:", error.Error())
					return false
				}

				mapedFile.WrotePostion = mfq.mapedFileSize
				mapedFile.CommittedPosition = mfq.mapedFileSize
				mapedFile.MByteBuffer.WritePos = int(mapedFile.WrotePostion)
				mfq.mapedFiles.PushBack(mapedFile)
				logger.Infof("load mapfiled %v success.", mapedFile.fileName)
			}
		}
	}

	return true
}

// HowMuchFallBehind 刷盘进度落后了多少
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/8
func (mfq *MapedFileQueue) HowMuchFallBehind() int64 {
	if mfq.mapedFiles.Len() == 0 {
		return 0
	}
	committed := mfq.committedWhere
	if committed != 0 {
		mapedFile, error := mfq.GetLastMapedFile(0)
		if error != nil {
			logger.Error(error.Error())
		}
		return mapedFile.fileFromOffset + mapedFile.WrotePostion - committed
	}
	return 0
}

// GetLastMapedFile 获取最后一个MapedFile对象，如果一个都没有，则新创建一个，
// 如果最后一个写满了，则新创建一个
// Params: startOffset 如果创建新的文件，起始offset
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/8
func (mfq *MapedFileQueue) GetLastMapedFile(startOffset int64) (*MapedFile, error) {
	var createOffset int64 = -1
	var mapedFile, mapedFileLast *MapedFile
	mfq.rwLock.RLock()
	if mfq.mapedFiles.Len() == 0 {
		createOffset = startOffset - (startOffset % int64(mfq.mapedFileSize))
	} else {
		mapedFileLastObj := (mfq.mapedFiles.Back().Value).(*MapedFile)
		mapedFileLast = mapedFileLastObj
	}
	mfq.rwLock.RUnlock()
	if mapedFileLast != nil && mapedFileLast.isFull() {
		createOffset = mapedFileLast.fileFromOffset + mfq.mapedFileSize
	}

	if createOffset != -1 {
		nextPath := mfq.storePath + string(filepath.Separator) + Offset2FileName(createOffset)
		nextNextPath := mfq.storePath + string(filepath.Separator) +
			Offset2FileName(createOffset+mfq.mapedFileSize)
		if mfq.allocateMFStrategy != nil {
			var err error
			mapedFile, err = mfq.allocateMFStrategy.PutRequestAndReturnMapedFile(nextPath, nextNextPath, mfq.mapedFileSize)
			if err != nil {
				logger.Errorf("put request and return maped file, error:%s ", err.Error())
				return nil, err
			}
		} else {
			var err error
			mapedFile, err = NewMapedFile(nextPath, mfq.mapedFileSize)
			if err != nil {
				logger.Errorf("maped file create maped file error: %s", err.Error())
				return nil, err
			}
		}

		if mapedFile != nil {
			mfq.rwLock.Lock()
			if mfq.mapedFiles.Len() == 0 {
				mapedFile.firstCreateInQueue = true
			}
			mfq.mapedFiles.PushBack(mapedFile)
			mfq.rwLock.Unlock()
		}

		return mapedFile, nil
	}

	return mapedFileLast, nil
}

func (mfq *MapedFileQueue) GetMinOffset() int64 {
	mfq.rwLock.RLock()
	defer mfq.rwLock.RUnlock()
	if mfq.mapedFiles.Len() > 0 {
		mappedfile := mfq.mapedFiles.Front().Value.(MapedFile)
		return mappedfile.fileFromOffset
	}

	return -1
}

func (mfq *MapedFileQueue) GetMaxOffset() int64 {
	mfq.rwLock.RLock()
	defer mfq.rwLock.RUnlock()
	if mfq.mapedFiles.Len() > 0 {
		mappedfile := mfq.mapedFiles.Back().Value.(*MapedFile)
		return mappedfile.fileFromOffset + mappedfile.WrotePostion
	}

	return 0
}

// DeleteLastMapedFile 恢复时调用
func (mfq *MapedFileQueue) DeleteLastMapedFile() {
	if mfq.mapedFiles.Len() != 0 {
		last := mfq.mapedFiles.Back()
		lastMapedFile := last.Value.(*MapedFile)
		lastMapedFile.Destroy(1000)
		mfq.mapedFiles.Remove(last)
		logger.Infof("on recover, destroy a logic maped file %v", lastMapedFile.fileName)
	}
}

// DeleteExpiredFileByTime 根据文件过期时间来删除物理队列文件
// Return: 删除过期文件的数量
// Author: tantexian, <tantexian@qq.com>
// Since: 17/8/9
func (mfq *MapedFileQueue) DeleteExpiredFileByTime(expiredTime int64, deleteFilesInterval int,
	intervalForcibly int64, cleanImmediately bool) int {
	// 获取当前MapedFiles列表中所有元素副本的切片
	files := mfq.copyMapedFiles(0)
	if len(files) == 0 {
		return 0
	}
	toBeDeleteMfList := list.New()
	var delCount int = 0
	// 最后一个文件处于写状态，不能删除
	mfsLength := len(files) - 1
	for i := 0; i < mfsLength; i++ {
		mf := files[i]
		if mf != nil {
			liveMaxTimestamp := mf.storeTimestamp + expiredTime
			if CurrentTimeMillis() > liveMaxTimestamp || cleanImmediately {
				if mf.Destroy(intervalForcibly) {
					toBeDeleteMfList.PushBack(mf)
					delCount++
					// 每次触发删除文件，最多删除多少个文件
					if toBeDeleteMfList.Len() >= mfq.deleteFilesBatchMax {
						break
					}
					// 删除最后一个文件不需要等待
					if deleteFilesInterval > 0 && (i+1) < mfsLength {
						time.Sleep(time.Second * time.Duration(deleteFilesInterval))
					}
				} else {
					break
				}
			}
		}
	}

	mfq.DeleteExpiredFile(toBeDeleteMfList)

	return delCount
}

// DeleteExpiredFileByOffset 根据物理队列最小Offset来删除逻辑队列
// Params: offset 物理队列最小offset
// Params: unitsize ???
// Author: tantexian, <tantexian@qq.com>
// Since: 17/8/9
func (mfq *MapedFileQueue) DeleteExpiredFileByOffset(offset int64, unitsize int) int {
	toBeDeleteFileList := list.New()
	deleteCount := 0
	mfs := mfq.copyMapedFiles(0)

	if mfs != nil && len(mfs) > 0 {
		// 最后一个文件处于写状态，不能删除
		mfsLength := len(mfs) - 1

		for i := 0; i < mfsLength; i++ {
			destroy := true
			mf := mfs[i]

			if mf == nil {
				continue
			}

			result := mf.SelectMapedBuffer(mfq.mapedFileSize - int64(unitsize))

			if result != nil {
				maxOffsetInLogicQueue := result.MappedByteBuffer.ReadInt64()
				result.Release()
				// 当前文件是否可以删除
				destroy = maxOffsetInLogicQueue < offset
				if destroy {
					logger.Infof("physic min offset %d, logics in current mapedfile max offset %d, delete it",
						offset, maxOffsetInLogicQueue)
				}
			} else {
				logger.Warn("this being not excuted forever.")
				break
			}

			if destroy && mf.Destroy(1000*60) {
				toBeDeleteFileList.PushBack(mf)
				deleteCount++
			}
		}
	}

	mfq.DeleteExpiredFile(toBeDeleteFileList)
	return deleteCount
}

func (mfq *MapedFileQueue) Commit(flushLeastPages int32) bool {
	result := true

	mapedFile := mfq.FindMapedFileByOffset(mfq.committedWhere, true)
	if mapedFile != nil {
		tmpTimeStamp := mapedFile.storeTimestamp
		offset := mapedFile.Commit(flushLeastPages)
		where := mapedFile.fileFromOffset + offset
		result = (where == mfq.committedWhere)
		mfq.committedWhere = where

		if 0 == flushLeastPages {
			mfq.storeTimestamp = tmpTimeStamp
		}
	}

	return result
}

func (mfq *MapedFileQueue) getFirstMapedFile() *MapedFile {
	if mfq.mapedFiles.Len() == 0 {
		return nil
	}

	element := mfq.mapedFiles.Front()
	result := element.Value.(*MapedFile)

	return result
}

func (mfq *MapedFileQueue) GetLastMapedFile2() *MapedFile {
	if mfq.mapedFiles.Len() == 0 {
		return nil
	}

	lastElement := mfq.mapedFiles.Back()
	result, ok := lastElement.Value.(*MapedFile)
	if !ok {
		logger.Info("mapedfile queue get last maped file type conversion error")
	}

	return result
}

// FindMapedFileByOffset
func (mfq *MapedFileQueue) FindMapedFileByOffset(offset int64, returnFirstOnNotFound bool) *MapedFile {
	mfq.rwLock.RLock()
	defer mfq.rwLock.RUnlock()

	mapedFile := mfq.getFirstMapedFile()
	if mapedFile != nil {
		index := (offset / mfq.mapedFileSize) - (mapedFile.fileFromOffset / mfq.mapedFileSize)
		if index < 0 || index >= int64(mfq.mapedFiles.Len()) {
			logger.Warnf("maped file queue find maped file by offset, offset not matched, request Offset: %d, index: %d, mapedFileSize: %d, mapedFiles count: %d",
				offset, index, mfq.mapedFileSize, mfq.mapedFiles.Len())
		}

		i := 0
		for e := mfq.mapedFiles.Front(); e != nil; e = e.Next() {
			if i == int(index) {
				result := e.Value.(*MapedFile)
				return result
			}
			i++
		}

		if returnFirstOnNotFound {
			return mapedFile
		}

	}

	return nil
}

func (mfq *MapedFileQueue) getLastAndLastMapedFile() *MapedFile {
	if mfq.mapedFiles.Len() == 0 {
		return nil
	}

	element := mfq.mapedFiles.Back()
	result := element.Value.(*MapedFile)

	return result
}

func (mfq *MapedFileQueue) getMapedMemorySize() int64 {
	return 0
}

func (mfq *MapedFileQueue) retryDeleteFirstFile(intervalForcibly int64) bool {
	mapFile := mfq.GetFirstMapedFileOnLock()
	if mapFile != nil {
		if !mapFile.isAvailable() {
			logger.Warn("the mapedfile was destroyed once, but still alive, ", mapFile.fileName)

			result := mapFile.Destroy(intervalForcibly)
			if result {
				logger.Info("the mapedfile redelete OK, ", mapFile.fileName)
				tmps := list.New()
				tmps.PushBack(mapFile)
				mfq.DeleteExpiredFile(tmps)
			} else {
				logger.Warn("the mapedfile redelete Failed, ", mapFile.fileName)
			}

			return result
		}
	}

	return false
}

func (mfq *MapedFileQueue) GetFirstMapedFileOnLock() *MapedFile {
	mfq.rwLock.RLock()
	defer mfq.rwLock.RUnlock()
	return mfq.getFirstMapedFile()
}

// shutdown 关闭队列，队列数据还在，但是不能访问
func (mfq *MapedFileQueue) shutdown(intervalForcibly int64) {

}

// Destroy 销毁队列，队列数据被删除，此函数有可能不成功
func (mfq *MapedFileQueue) Destroy() {
	mfq.rwLock.Lock()
	defer mfq.rwLock.Unlock()

	for element := mfq.mapedFiles.Front(); element != nil; element = element.Next() {
		mapedFile, ok := element.Value.(*MapedFile)
		if !ok {
			logger.Warnf("maped file queue destroy type conversion error")
			continue
		}
		mapedFile.Destroy(1000 * 3)
	}

	mfq.mapedFiles.Init()
	mfq.committedWhere = 0

	// delete parent director
	exist, err := PathExists(mfq.storePath)
	if err != nil {
		logger.Warn("maped file queue destroy check store path is exists, error:", err.Error())
	}

	if exist {
		if storeFile, _ := os.Stat(mfq.storePath); storeFile.IsDir() {
			os.RemoveAll(mfq.storePath)
		}
	}
}

// MapedFiles 返回mapedFiles
func (mfq *MapedFileQueue) MapedFiles() *list.List {
	return mfq.mapedFiles
}

// MapedFileSize 返回mapedFileSize
func (mfq *MapedFileQueue) MapedFileSize() int64 {
	return mfq.mapedFileSize
}

// CommittedWhere 返回committedWhere
func (mfq *MapedFileQueue) CommittedWhere() int64 {
	return mfq.committedWhere
}

// StoreTimestamp 返回storeTimestamp
func (mfq *MapedFileQueue) StoreTimestamp() int64 {
	return mfq.storeTimestamp
}

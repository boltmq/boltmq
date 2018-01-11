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
	"container/list"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/system"
)

// allocateMappedFileStrategy 分配mapped file方式
type allocateMappedFileStrategy interface {
	putRequestAndReturnMappedFile(nextFilePath string, nextNextFilePath string, filesize int64) (*mappedFile, error)
}

type mappedFileQueue struct {
	// 每次触发删除文件，最多删除多少个文件
	deleteFilesBatchMax int
	// 文件存储位置
	storePath string
	// 每个文件的大小
	mappedFileSize int64
	// 各个文件
	mappedFiles *list.List
	// 读写锁（针对mappedFiles）
	rwLock sync.RWMutex
	// 预分配MappedFile对象服务
	allocateMFStrategy allocateMappedFileStrategy
	// 刷盘刷到哪里
	committedWhere int64
	// 最后一条消息存储时间
	storeTimestamp int64
}

// newMappedFileQueue create mapped file queue
func newMappedFileQueue(storePath string, mappedFileSize int64,
	allocateMFStrategy allocateMappedFileStrategy) *mappedFileQueue {
	mfq := &mappedFileQueue{}
	mfq.storePath = storePath           // 存储路径
	mfq.mappedFileSize = mappedFileSize // 文件size
	mfq.mappedFiles = list.New()
	// 根据读写请求队列requestQueue/readQueue中的读写请求，创建对应的mappedFile文件
	mfq.allocateMFStrategy = allocateMFStrategy
	mfq.deleteFilesBatchMax = 10
	mfq.committedWhere = 0
	mfq.storeTimestamp = 0

	return mfq
}

func (mfq *mappedFileQueue) getMappedFileByTime(timestamp int64) (mf *mappedFile) {
	mappedFileSlice := mfq.copyMappedFiles(0)
	if mappedFileSlice == nil {
		return nil
	}

	for _, mf := range mappedFileSlice {
		if mf != nil {
			fileInfo, err := os.Stat(mf.fileName)
			if err != nil {
				logger.Warnf("mapped file queue get mapped file by time error: %s.", err)
				continue
			}

			modifiedTime := fileInfo.ModTime().UnixNano() / 1000000
			if modifiedTime >= timestamp {
				return mf
			}
		}
	}

	return mappedFileSlice[len(mappedFileSlice)-1]
}

// copyMappedFiles 获取当前mappedFiles列表中的副本slice
// Params: reservedMappedFiles 只有当mappedFiles列表中文件个数大于reservedMappedFiles才返回副本
// Return: 返回大于等于reservedMappedFiles个元数的MappedFile切片
// Author: tantexian, <tantexian@qq.com>
// Since: 17/8/9
func (mfq *mappedFileQueue) copyMappedFiles(reservedMappedFiles int) []*mappedFile {
	mappedFileSlice := make([]*mappedFile, mfq.mappedFiles.Len())
	mfq.rwLock.RLock()
	defer mfq.rwLock.RUnlock()
	if mfq.mappedFiles.Len() <= reservedMappedFiles {
		return nil
	}

	// Iterate through list and print its contents.
	for e := mfq.mappedFiles.Front(); e != nil; e = e.Next() {
		mf := e.Value.(*mappedFile)
		if mf != nil {
			mappedFileSlice = append(mappedFileSlice, mf)
		}
	}

	return mappedFileSlice
}

// truncateDirtyFiles recover时调用，不需要加锁
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/7
func (mfq *mappedFileQueue) truncateDirtyFiles(offset int64) {
	willRemoveFiles := list.New()

	// Iterate through list and print its contents.
	for e := mfq.mappedFiles.Front(); e != nil; e = e.Next() {
		mf := e.Value.(*mappedFile)
		fileTailOffset := mf.fileFromOffset + mf.fileSize
		if fileTailOffset > offset {
			if offset >= mf.fileFromOffset {
				pos := offset % int64(mfq.mappedFileSize)
				mf.wrotePostion = pos
				mf.byteBuffer.writePos = int(pos)
				mf.committedPosition = pos
			} else {
				mf.destroy(1000)
				willRemoveFiles.PushBack(mf)
			}
		}
	}
	mfq.deleteExpiredFile(willRemoveFiles)
}

// deleteExpiredFile 删除过期文件只能从头开始删
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/7
func (mfq *mappedFileQueue) deleteExpiredFile(mfs *list.List) {
	if mfs != nil && mfs.Len() > 0 {
		mfq.rwLock.Lock()
		defer mfq.rwLock.Unlock()
		for de := mfs.Front(); de != nil; de = de.Next() {
			for e := mfq.mappedFiles.Front(); e != nil; e = e.Next() {
				deleteFile := de.Value.(*mappedFile)
				file := e.Value.(*mappedFile)

				if deleteFile.fileName == file.fileName {
					success := mfq.mappedFiles.Remove(e)
					if success == false {
						logger.Error("delete expired file remove failed.")
					}
				}
			}
		}
	}
}

// load 从磁盘加载mappedfile到内存映射
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/8
func (mfq *mappedFileQueue) load() bool {
	exist, err := common.PathExists(mfq.storePath)
	if err != nil {
		logger.Infof("mapped file queue load store path error: %s.", err)
		return false
	}

	if exist {
		files, err := listFilesOrDir(mfq.storePath, "FILE")
		if err != nil {
			logger.Errorf("mapped file queue list dir err: %s.", err)
			return false
		}
		if len(files) > 0 {
			// 按照文件名，升序排列
			sort.Strings(files)
			for _, path := range files {
				file, err := os.Stat(path)
				if err != nil {
					logger.Errorf("mapped file queue load file %s err: %s.", path, err)
				}

				if file == nil {
					logger.Errorf("mapped file queue load file not exist: %s.", path)
				}

				size := file.Size()
				// 校验文件大小是否匹配
				if size != int64(mfq.mappedFileSize) {
					logger.Warnf("filesize(%d) mappedfile-size(%d) length not matched message store config value, ignore it.", size, mfq.mappedFileSize)
					return true
				}

				// 恢复队列
				mf, err := newMappedFile(path, int64(mfq.mappedFileSize))
				if err != nil {
					logger.Errorf("mapped file queue load file err: %s.", err)
					return false
				}

				mf.wrotePostion = mfq.mappedFileSize
				mf.committedPosition = mfq.mappedFileSize
				mf.byteBuffer.writePos = int(mf.wrotePostion)
				mfq.mappedFiles.PushBack(mf)
				logger.Infof("load mapfiled %s success.", mf.fileName)
			}
		}
	}

	return true
}

// howMuchFallBehind 刷盘进度落后了多少
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/8
func (mfq *mappedFileQueue) howMuchFallBehind() int64 {
	if mfq.mappedFiles.Len() == 0 {
		return 0
	}
	committed := mfq.committedWhere
	if committed != 0 {
		mf, err := mfq.getLastMappedFile(0)
		if err != nil {
			logger.Error("mapped file queue get last file err: %s.", err)
		}
		return mf.fileFromOffset + mf.wrotePostion - committed
	}
	return 0
}

// getLastMappedFile 获取最后一个MappedFile对象，如果一个都没有，则新创建一个，
// 如果最后一个写满了，则新创建一个
// Params: startOffset 如果创建新的文件，起始offset
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/8
func (mfq *mappedFileQueue) getLastMappedFile(startOffset int64) (*mappedFile, error) {
	var createOffset int64 = -1
	var mf, mappedFileLast *mappedFile
	mfq.rwLock.RLock()
	if mfq.mappedFiles.Len() == 0 {
		createOffset = startOffset - (startOffset % int64(mfq.mappedFileSize))
	} else {
		mappedFileLastObj := (mfq.mappedFiles.Back().Value).(*mappedFile)
		mappedFileLast = mappedFileLastObj
	}
	mfq.rwLock.RUnlock()
	if mappedFileLast != nil && mappedFileLast.isFull() {
		createOffset = mappedFileLast.fileFromOffset + mfq.mappedFileSize
	}

	if createOffset != -1 {
		nextPath := mfq.storePath + string(filepath.Separator) + offset2FileName(createOffset)
		nextNextPath := mfq.storePath + string(filepath.Separator) +
			offset2FileName(createOffset+mfq.mappedFileSize)
		if mfq.allocateMFStrategy != nil {
			var err error
			mf, err = mfq.allocateMFStrategy.putRequestAndReturnMappedFile(nextPath, nextNextPath, mfq.mappedFileSize)
			if err != nil {
				logger.Errorf("put request and return mapped file, error: %s.", err)
				return nil, err
			}
		} else {
			var err error
			mf, err = newMappedFile(nextPath, mfq.mappedFileSize)
			if err != nil {
				logger.Errorf("mapped file create mapped file error: %s.", err)
				return nil, err
			}
		}

		if mf != nil {
			mfq.rwLock.Lock()
			if mfq.mappedFiles.Len() == 0 {
				mf.firstCreateInQueue = true
			}
			mfq.mappedFiles.PushBack(mf)
			mfq.rwLock.Unlock()
		}

		return mf, nil
	}

	return mappedFileLast, nil
}

func (mfq *mappedFileQueue) getMinOffset() int64 {
	mfq.rwLock.RLock()
	defer mfq.rwLock.RUnlock()
	if mfq.mappedFiles.Len() > 0 {
		mf := mfq.mappedFiles.Front().Value.(*mappedFile)
		return mf.fileFromOffset
	}

	return -1
}

func (mfq *mappedFileQueue) getMaxOffset() int64 {
	mfq.rwLock.RLock()
	defer mfq.rwLock.RUnlock()
	if mfq.mappedFiles.Len() > 0 {
		mf := mfq.mappedFiles.Back().Value.(*mappedFile)
		return mf.fileFromOffset + mf.wrotePostion
	}

	return 0
}

// deleteLastMappedFile 恢复时调用
func (mfq *mappedFileQueue) deleteLastMappedFile() {
	if mfq.mappedFiles.Len() != 0 {
		last := mfq.mappedFiles.Back()
		lastMappedFile := last.Value.(*mappedFile)
		lastMappedFile.destroy(1000)
		mfq.mappedFiles.Remove(last)
		logger.Infof("on recover, destroy a logic mapped file %s.", lastMappedFile.fileName)
	}
}

// deleteExpiredFileByTime 根据文件过期时间来删除物理队列文件
// Return: 删除过期文件的数量
// Author: tantexian, <tantexian@qq.com>
// Since: 17/8/9
func (mfq *mappedFileQueue) deleteExpiredFileByTime(expiredTime int64, deleteFilesInterval int,
	intervalForcibly int64, cleanImmediately bool) int {
	// 获取当前MappedFiles列表中所有元素副本的切片
	files := mfq.copyMappedFiles(0)
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
			if system.CurrentTimeMillis() > liveMaxTimestamp || cleanImmediately {
				if mf.destroy(intervalForcibly) {
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

	mfq.deleteExpiredFile(toBeDeleteMfList)
	return delCount
}

// deleteExpiredFileByOffset 根据物理队列最小Offset来删除逻辑队列
// Params: offset 物理队列最小offset
// Params: unitsize ???
// Author: tantexian, <tantexian@qq.com>
// Since: 17/8/9
func (mfq *mappedFileQueue) deleteExpiredFileByOffset(offset int64, unitsize int) int {
	toBeDeleteFileList := list.New()
	deleteCount := 0
	mfs := mfq.copyMappedFiles(0)

	if mfs != nil && len(mfs) > 0 {
		// 最后一个文件处于写状态，不能删除
		mfsLength := len(mfs) - 1

		for i := 0; i < mfsLength; i++ {
			destroy := true
			mf := mfs[i]

			if mf == nil {
				continue
			}

			result := mf.selectMappedBuffer(mfq.mappedFileSize - int64(unitsize))

			if result != nil {
				maxOffsetInLogicQueue := result.byteBuffer.ReadInt64()
				result.Release()
				// 当前文件是否可以删除
				destroy = maxOffsetInLogicQueue < offset
				if destroy {
					logger.Infof("physic min offset %d, logics in current mappedfile max offset %d, delete it.",
						offset, maxOffsetInLogicQueue)
				}
			} else {
				logger.Warn("this being not excuted forever.")
				break
			}

			if destroy && mf.destroy(1000*60) {
				toBeDeleteFileList.PushBack(mf)
				deleteCount++
			}
		}
	}

	mfq.deleteExpiredFile(toBeDeleteFileList)
	return deleteCount
}

func (mfq *mappedFileQueue) commit(flushLeastPages int32) bool {
	result := true

	mf := mfq.findMappedFileByOffset(mfq.committedWhere, true)
	if mf != nil {
		tmpTimeStamp := mf.storeTimestamp
		offset := mf.commit(flushLeastPages)
		where := mf.fileFromOffset + offset
		result = (where == mfq.committedWhere)
		mfq.committedWhere = where

		if 0 == flushLeastPages {
			mfq.storeTimestamp = tmpTimeStamp
		}
	}

	return result
}

func (mfq *mappedFileQueue) getFirstMappedFile() *mappedFile {
	if mfq.mappedFiles.Len() == 0 {
		return nil
	}

	element := mfq.mappedFiles.Front()
	result := element.Value.(*mappedFile)

	return result
}

func (mfq *mappedFileQueue) getLastMappedFile2() *mappedFile {
	if mfq.mappedFiles.Len() == 0 {
		return nil
	}

	lastElement := mfq.mappedFiles.Back()
	result, ok := lastElement.Value.(*mappedFile)
	if !ok {
		logger.Info("mappedfile queue get last mapped file type conversion error.")
	}

	return result
}

// findMappedFileByOffset
func (mfq *mappedFileQueue) findMappedFileByOffset(offset int64, returnFirstOnNotFound bool) *mappedFile {
	mfq.rwLock.RLock()
	defer mfq.rwLock.RUnlock()

	mf := mfq.getFirstMappedFile()
	if mf != nil {
		index := (offset / mfq.mappedFileSize) - (mf.fileFromOffset / mfq.mappedFileSize)
		if index < 0 || index >= int64(mfq.mappedFiles.Len()) {
			logger.Warnf("mapped file queue find mapped file by offset, offset not matched, request Offset: %d, index: %d, mappedFileSize: %d, mappedFiles count: %d.",
				offset, index, mfq.mappedFileSize, mfq.mappedFiles.Len())
		}

		i := 0
		for e := mfq.mappedFiles.Front(); e != nil; e = e.Next() {
			if i == int(index) {
				result := e.Value.(*mappedFile)
				return result
			}
			i++
		}

		if returnFirstOnNotFound {
			return mf
		}

	}

	return nil
}

func (mfq *mappedFileQueue) getLastAndLastMappedFile() *mappedFile {
	if mfq.mappedFiles.Len() == 0 {
		return nil
	}

	element := mfq.mappedFiles.Back()
	result := element.Value.(*mappedFile)

	return result
}

func (mfq *mappedFileQueue) getMappedMemorySize() int64 {
	return 0
}

func (mfq *mappedFileQueue) retryDeleteFirstFile(intervalForcibly int64) bool {
	mapFile := mfq.getFirstMappedFileOnLock()
	if mapFile != nil {
		if !mapFile.isAvailable() {
			logger.Warnf("the mappedfile was destroyed once, but still alive, %s.", mapFile.fileName)

			result := mapFile.destroy(intervalForcibly)
			if result {
				logger.Infof("the mappedfile redelete success, %s.", mapFile.fileName)
				tmps := list.New()
				tmps.PushBack(mapFile)
				mfq.deleteExpiredFile(tmps)
			} else {
				logger.Warnf("the mappedfile redelete failed, %s.", mapFile.fileName)
			}

			return result
		}
	}

	return false
}

func (mfq *mappedFileQueue) getFirstMappedFileOnLock() *mappedFile {
	mfq.rwLock.RLock()
	defer mfq.rwLock.RUnlock()
	return mfq.getFirstMappedFile()
}

// shutdown 关闭队列，队列数据还在，但是不能访问
func (mfq *mappedFileQueue) shutdown(intervalForcibly int64) {

}

// destroy 销毁队列，队列数据被删除，此函数有可能不成功
func (mfq *mappedFileQueue) destroy() {
	mfq.rwLock.Lock()
	defer mfq.rwLock.Unlock()

	for element := mfq.mappedFiles.Front(); element != nil; element = element.Next() {
		mf, ok := element.Value.(*mappedFile)
		if !ok {
			logger.Warnf("mapped file queue destroy type conversion error.")
			continue
		}
		mf.destroy(1000 * 3)
	}

	mfq.mappedFiles.Init()
	mfq.committedWhere = 0

	// delete parent director
	exist, err := common.PathExists(mfq.storePath)
	if err != nil {
		logger.Warnf("mapped file queue destroy check store path is exists, err: %s.", err)
	}

	if exist {
		if storeFile, _ := os.Stat(mfq.storePath); storeFile.IsDir() {
			os.RemoveAll(mfq.storePath)
		}
	}
}

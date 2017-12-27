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
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltmq/boltmq/store/core/mmap"
	"github.com/boltmq/common/logger"
	"github.com/go-errors/errors"
)

const (
	OS_PAGE_SIZE       = 1024 * 4
	MMAPED_ENTIRE_FILE = -1
)

// MapedFile 封装mapedfile类用于操作commitlog文件及consumelog文件
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
type MapedFile struct {
	ReferenceResource
	// 当前映射的虚拟内存总大小
	TotalMapedVitualMemory int64
	// 当前JVM中mmap句柄数量
	TotalMapedFiles int32
	// 映射的文件名
	fileName string
	// 映射的起始偏移量
	fileFromOffset int64
	// 映射的文件大小，定长
	fileSize int64
	// 映射的文件
	file *os.File
	// 映射的内存对象，position永远不变
	MByteBuffer *MappedByteBuffer
	//mmapBytes        mmap.MemoryMap
	// 当前写到什么位置
	WrotePostion int64
	// Flush到什么位置
	CommittedPosition int64
	// 最后一条消息存储时间
	storeTimestamp     int64
	firstCreateInQueue bool
	// 文件读写锁
	rwLock sync.RWMutex
}

// NewMapedFile 根据文件名新建mapedfile
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
func NewMapedFile(filePath string, filesize int64) (*MapedFile, error) {
	mapedFile := new(MapedFile)
	// 初始化ReferenceResource信息
	mapedFile.refCount = 1
	mapedFile.available = true
	mapedFile.cleanupOver = false
	mapedFile.firstShutdownTimestamp = 0

	mapedFile.fileName = filePath
	mapedFile.fileSize = filesize

	commitRootDir := ParentDirectory(filePath)
	err := EnsureDir(commitRootDir)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	exist, err := PathExists(filePath)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666)
	defer file.Close()
	if err != nil {
		logger.Error(err.Error())
		return nil, errors.Wrap(err, 0)
	}
	mapedFile.file = file

	if exist == false {
		// 如果文件不存在则新建filesize大小文件
		if err := os.Truncate(filePath, filesize); err != nil {
			logger.Error("maped file set file size error:", err.Error())
			return nil, errors.Wrap(err, 0)
		}
	}

	fileName := filepath.Base(mapedFile.file.Name())

	// 文件名即offset起始地址
	offset, err := strconv.ParseInt(fileName, 10, 64)
	if err != nil {
		logger.Error(err.Error())
		return nil, errors.Wrap(err, 0)
	}
	mapedFile.fileFromOffset = offset

	mmapBytes, err := mmap.MapRegion(file, MMAPED_ENTIRE_FILE, mmap.RDWR, 0, 0)
	if err != nil {
		logger.Error(err.Error())
		//return nil, err
	}

	mapedFile.MByteBuffer = NewMappedByteBuffer(mmapBytes)
	atomic.AddInt64(&mapedFile.TotalMapedVitualMemory, int64(filesize))
	atomic.AddInt32(&mapedFile.TotalMapedFiles, 1)

	return mapedFile, nil
}

// AppendMessageWithCallBack 向MapedBuffer追加消息
// Return: appendNums 成功添加消息字节数
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
func (mf *MapedFile) AppendMessageWithCallBack(msg interface{}, appendMessageCallback AppendMessageCallback) *AppendMessageResult {
	if msg == nil {
		logger.Error("AppendMessage nil msg error!!!")
		return nil
	}

	curPos := atomic.LoadInt64(&mf.WrotePostion)
	// 表示还有剩余空间
	if curPos < mf.fileSize {
		result := appendMessageCallback.DoAppend(mf.fileFromOffset, mf.MByteBuffer, int32(mf.fileSize)-int32(curPos), msg)
		atomic.AddInt64(&mf.WrotePostion, int64(result.WroteBytes))
		mf.storeTimestamp = result.StoreTimestamp
		return result
	}

	// TODO: 上层应用应该保证不会走到这里
	logger.Errorf("MapedFile.appendMessage return null, WrotePostion:%d fileSize:%d", curPos, mf.fileSize)
	return &AppendMessageResult{Status: APPENDMESSAGE_UNKNOWN_ERROR}
}

// AppendMessage 向存储层追加数据，一般在SLAVE存储结构中使用
// Params: data 追加数据
// Return: 追加是否成功
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/6
func (mf *MapedFile) AppendMessage(data []byte) bool {
	currPos := int64(mf.WrotePostion)
	if currPos+int64(len(data)) <= mf.fileSize {
		n, err := mf.MByteBuffer.Write(data)
		if err != nil {
			logger.Error("maped file append message error:", err.Error())
			return false
		}
		atomic.AddInt64(&mf.WrotePostion, int64(n))
		return true
	}

	return false
}

// Commit 消息提交刷盘
// Params: flushLeastPages 一次刷盘最少个数
// Return: flushPosition 当前刷盘位置
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/6
func (mf *MapedFile) Commit(flushLeastPages int32) (flushPosition int64) {
	if mf.isAbleToFlush(flushLeastPages) {
		if mf.hold() {
			mf.rwLock.Lock()           // 对文件加写锁
			currPos := mf.WrotePostion // 获取当前写的位置
			mf.Flush()                 // 将MByteBuffer的数据强制刷新到磁盘文件中
			//mf.mmapBytes
			mf.CommittedPosition = currPos // 刷新完毕，则将CommittedPosition即flush的位置更新为当前位置记录
			mf.rwLock.Unlock()             // 释放锁
			mf.release()
		} else {
			logger.Warn("in commit, hold failed, commit offset = ", atomic.LoadInt64(&mf.CommittedPosition))
			mf.CommittedPosition = atomic.LoadInt64(&mf.WrotePostion)
		}
	}

	return mf.CommittedPosition
}

// Flush
func (mf *MapedFile) Flush() {
	mf.MByteBuffer.flush()
}

// Unmap
func (mf *MapedFile) Unmap() {
	atomic.AddInt64(&mf.TotalMapedVitualMemory, -int64(mf.fileSize))
	atomic.AddInt32(&mf.TotalMapedFiles, -1)
	mf.MByteBuffer.unmap()
}

// isAbleToFlush 根据最少需要刷盘page数值来判断当前是否需要立即刷新缓存数据到磁盘
// Params: flushLeastPages一次刷盘最少个数
// Return: 是否需要立即刷盘
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/6
func (mf *MapedFile) isAbleToFlush(flushLeastPages int32) bool {
	// 获取当前flush到磁盘的位置
	flush := mf.CommittedPosition
	// 获取当前write到缓冲区的位置
	write := mf.WrotePostion
	if mf.isFull() {

		return true
	}
	// 只有未刷盘数据满足指定page数目才刷盘
	// OS_PAGE_SIZE默认为1024*4=4k
	if flushLeastPages > 0 {
		// 计算出前期写缓冲区的位置到已刷盘的数据位置之间的数据，是否大于等于设置的至少得刷盘page个数
		// 超过则需要刷盘
		return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= int64(flushLeastPages)
	}

	// 如果flushLeastPages为0，那么则是每次有数据写入缓冲区则则直接刷盘
	return write > flush
}

func (mf *MapedFile) isFull() bool {
	return mf.fileSize == int64(mf.WrotePostion)
}

func (mf *MapedFile) Destroy(intervalForcibly int64) bool {
	mf.shutdown(intervalForcibly)

	if mf.isCleanupOver() {
		mf.Unmap()
		mf.file.Close()
		logger.Infof("close file %s OK", mf.fileName)

		if err := os.Remove(mf.file.Name()); err != nil {
			logger.Errorf("message store delete file %s error: ", mf.file.Name(), err.Error())
			return false
		}
	}

	return true
}

func (mf *MapedFile) shutdown(intervalForcibly int64) {
	if mf.available {
		mf.available = false
		mf.firstShutdownTimestamp = time.Now().UnixNano() / 1000000
		mf.release()
	} else if mf.refCount > 0 { // 强制shutdown
		if (time.Now().UnixNano()/1000000 - mf.firstShutdownTimestamp) >= intervalForcibly {
			mf.refCount = -1000 - mf.refCount
			mf.release()
		}
	}
}

func (mf *MapedFile) SelectMapedBuffer(pos int64) *SelectMapedBufferResult {
	if pos < mf.WrotePostion && pos >= 0 {
		if mf.hold() {
			size := mf.MByteBuffer.WritePos - int(pos)
			if mf.MByteBuffer.WritePos > len(mf.MByteBuffer.MMapBuf) {
				return nil
			}

			newMmpBuffer := NewMappedByteBuffer(mf.MByteBuffer.Bytes())
			newMmpBuffer.WritePos = mf.MByteBuffer.WritePos
			newMmpBuffer.ReadPos = int(pos)
			return newSelectMapedBufferResult(mf.fileFromOffset+pos, newMmpBuffer, int32(size), mf)
		}
	}

	return nil
}

func (mf *MapedFile) SelectMapedBufferByPosAndSize(pos int64, size int32) *SelectMapedBufferResult {
	if (pos + int64(size)) <= mf.WrotePostion {
		if mf.hold() {
			end := pos + int64(size)
			if end > int64(len(mf.MByteBuffer.MMapBuf)) {
				return nil
			}

			byteBuffer := NewMappedByteBuffer(mf.MByteBuffer.MMapBuf[pos:end])
			byteBuffer.WritePos = int(size)
			return newSelectMapedBufferResult(mf.fileFromOffset+pos, byteBuffer, size, mf)
		} else {
			logger.Warn("matched, but hold failed, request pos: %d, fileFromOffset: %d", pos, mf.fileFromOffset)
		}
	} else {
		logger.Warnf("SelectMapedBuffer request pos invalid, request pos: %d, size: %d, fileFromOffset: %d",
			pos, size, mf.fileFromOffset)
	}

	return nil
}

func (mf *MapedFile) cleanup(currentRef int64) bool {
	// 如果没有被shutdown，则不可以unmap文件，否则会crash
	if mf.isAvailable() {
		logger.Errorf("this file[REF:%d] %s have cleanup, do not do it again.", currentRef, mf.fileName)
		return false
	}

	// 如果已经cleanup，再次操作会引起crash
	if mf.isCleanupOver() {
		logger.Errorf("this file[REF:%d] %s have cleanup, do not do it again.", currentRef, mf.fileName)
		return true
	}

	clean(mf.MByteBuffer)
	// TotalMapedVitualMemory
	// TotalMapedFiles
	logger.Infof("unmap file[REF:%d] %s OK", currentRef, mf.fileName)

	return true
}

// FileFromOffset return fileFromOffset
func (mf *MapedFile) FileFromOffset() int64 {
	return mf.fileFromOffset
}

// FileName return fileName
func (mf *MapedFile) FileName() string {
	return mf.fileName
}

// IsFirstCreateInQueue return firstCreateInQueue
func (mf *MapedFile) IsFirstCreateInQueue() bool {
	return mf.firstCreateInQueue
}

func (mf *MapedFile) release() {
	atomic.AddInt64(&mf.refCount, -1)
	if atomic.LoadInt64(&mf.refCount) > 0 {
		return
	}

	mf.rwLock.Lock()
	mf.cleanupOver = mf.cleanup(atomic.LoadInt64(&mf.refCount))
	mf.rwLock.Unlock()
}

func clean(mbb *MappedByteBuffer) {
	if mbb == nil {
		return
	}

	// TODO
	mbb.unmap()
}

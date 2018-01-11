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
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/boltmq/store"
	"github.com/boltmq/boltmq/store/persistent/mmap"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/system"
	"github.com/go-errors/errors"
)

const (
	OS_PAGE_SIZE       = 1024 * 4
	MMAPED_ENTIRE_FILE = -1
)

// appendMessageCallback 写消息回调接口
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/6
type appendMessageCallback interface {
	// write MappedByteBuffer,and return How many bytes to write
	doAppend(fileFromOffset int64, byteBuffer *mappedByteBuffer, maxBlank int32, msg interface{}) *store.AppendMessageResult
}

type referenceResource struct {
	refCount               int64
	available              bool
	cleanupOver            bool
	firstShutdownTimestamp int64
	mutexs                 sync.Mutex
}

func newReferenceResource() *referenceResource {
	return &referenceResource{
		refCount:               1,
		available:              true,
		cleanupOver:            false,
		firstShutdownTimestamp: 0,
	}
}

func (rf *referenceResource) hold() bool {
	rf.mutexs.Lock()
	defer rf.mutexs.Unlock()

	if rf.available {
		atomic.AddInt64(&rf.refCount, 1)
		if atomic.LoadInt64(&rf.refCount) > 0 {
			return true
		} else {
			atomic.AddInt64(&rf.refCount, -1)
		}
	}

	return false
}

func (rf *referenceResource) isAvailable() bool {
	return rf.available
}

func (rf *referenceResource) isCleanupOver() bool {
	return rf.refCount <= 0 && rf.cleanupOver
}

// mappedFile 封装mappedfile类用于操作commitlog文件及consumelog文件
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
type mappedFile struct {
	referenceResource
	// 当前映射的虚拟内存总大小
	totalMappedVitualMemory int64
	// 当前mmap句柄数量
	totalmappedFiles int32
	// 映射的文件名
	fileName string
	// 映射的起始偏移量
	fileFromOffset int64
	// 映射的文件大小，定长
	fileSize int64
	// 映射的文件
	file *os.File
	// 映射的内存对象，position永远不变
	byteBuffer *mappedByteBuffer
	//mmapBytes        mmap.MemoryMap
	// 当前写到什么位置
	wrotePostion int64
	// Flush到什么位置
	committedPosition int64
	// 最后一条消息存储时间
	storeTimestamp     int64
	firstCreateInQueue bool
	// 文件读写锁
	rwLock sync.RWMutex
}

// newMappedFile 根据文件名新建mappedfile
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
func newMappedFile(filePath string, filesize int64) (*mappedFile, error) {
	mf := new(mappedFile)
	// 初始化referenceResource信息
	mf.refCount = 1
	mf.available = true
	mf.cleanupOver = false
	mf.firstShutdownTimestamp = 0

	mf.fileName = filePath
	mf.fileSize = filesize

	commitRootDir := common.ParentDirectory(filePath)
	err := common.EnsureDir(commitRootDir)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	exist, err := common.PathExists(filePath)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666)
	defer file.Close()
	if err != nil {
		logger.Errorf("create mapped file err: %s.", err)
		return nil, errors.Wrap(err, 0)
	}
	mf.file = file

	if exist == false {
		// 如果文件不存在则新建filesize大小文件
		if err := os.Truncate(filePath, filesize); err != nil {
			logger.Errorf("mapped file set file size err: %s.", err)
			return nil, errors.Wrap(err, 0)
		}
	}

	fileName := filepath.Base(mf.file.Name())

	// 文件名即offset起始地址
	offset, err := strconv.ParseInt(fileName, 10, 64)
	if err != nil {
		logger.Errorf("mapped file name invalid. %s.", err)
		return nil, errors.Wrap(err, 0)
	}
	mf.fileFromOffset = offset

	mmapBytes, err := mmap.MapRegion(file, MMAPED_ENTIRE_FILE, mmap.RDWR, 0, 0)
	if err != nil {
		logger.Errorf("mapped file mapping err: %s.", err)
		return nil, err
	}

	mf.byteBuffer = newMappedByteBuffer(mmapBytes)
	atomic.AddInt64(&mf.totalMappedVitualMemory, int64(filesize))
	atomic.AddInt32(&mf.totalmappedFiles, 1)

	return mf, nil
}

// appendMessageWithCallBack 向MappedBuffer追加消息
// Return: appendNums 成功添加消息字节数
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
func (mf *mappedFile) appendMessageWithCallBack(msg interface{}, amcb appendMessageCallback) *store.AppendMessageResult {
	if msg == nil {
		logger.Error("append message is nil.")
		return nil
	}

	curPos := atomic.LoadInt64(&mf.wrotePostion)
	// 表示还有剩余空间
	if curPos < mf.fileSize {
		result := amcb.doAppend(mf.fileFromOffset, mf.byteBuffer, int32(mf.fileSize)-int32(curPos), msg)
		atomic.AddInt64(&mf.wrotePostion, int64(result.WroteBytes))
		mf.storeTimestamp = result.StoreTimestamp
		return result
	}

	// TODO: 上层应用应该保证不会走到这里
	logger.Errorf("mapped file append message return nil, wrotePostion:%d fileSize:%d.", curPos, mf.fileSize)
	return &store.AppendMessageResult{Status: store.APPENDMESSAGE_UNKNOWN_ERROR}
}

// appendMessage 向存储层追加数据，一般在SLAVE存储结构中使用
// Params: data 追加数据
// Return: 追加是否成功
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/6
func (mf *mappedFile) appendMessage(data []byte) bool {
	currPos := int64(mf.wrotePostion)
	if currPos+int64(len(data)) <= mf.fileSize {
		n, err := mf.byteBuffer.Write(data)
		if err != nil {
			logger.Errorf("mapped file append message err: %s.", err)
			return false
		}
		atomic.AddInt64(&mf.wrotePostion, int64(n))
		return true
	}

	return false
}

// commit 消息提交刷盘
// Params: flushLeastPages 一次刷盘最少个数
// Return: flushPosition 当前刷盘位置
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/6
func (mf *mappedFile) commit(flushLeastPages int32) (flushPosition int64) {
	if mf.isAbleToFlush(flushLeastPages) {
		if mf.hold() {
			mf.rwLock.Lock()           // 对文件加写锁
			currPos := mf.wrotePostion // 获取当前写的位置
			mf.flush()                 // 将byteBuffer的数据强制刷新到磁盘文件中
			//mf.mmapBytes
			mf.committedPosition = currPos // 刷新完毕，则将committedPosition即flush的位置更新为当前位置记录
			mf.rwLock.Unlock()             // 释放锁
			mf.release()
		} else {
			logger.Warnf("in commit, hold failed, commitoffset=%s", atomic.LoadInt64(&mf.committedPosition))
			mf.committedPosition = atomic.LoadInt64(&mf.wrotePostion)
		}
	}

	return mf.committedPosition
}

func (mf *mappedFile) flush() {
	mf.byteBuffer.flush()
}

func (mf *mappedFile) unmap() {
	atomic.AddInt64(&mf.totalMappedVitualMemory, -int64(mf.fileSize))
	atomic.AddInt32(&mf.totalmappedFiles, -1)
	mf.byteBuffer.unmap()
}

// isAbleToFlush 根据最少需要刷盘page数值来判断当前是否需要立即刷新缓存数据到磁盘
// Params: flushLeastPages一次刷盘最少个数
// Return: 是否需要立即刷盘
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/6
func (mf *mappedFile) isAbleToFlush(flushLeastPages int32) bool {
	// 获取当前flush到磁盘的位置
	flush := mf.committedPosition
	// 获取当前write到缓冲区的位置
	write := mf.wrotePostion
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

func (mf *mappedFile) isFull() bool {
	return mf.fileSize == int64(mf.wrotePostion)
}

func (mf *mappedFile) destroy(intervalForcibly int64) bool {
	mf.shutdown(intervalForcibly)

	if mf.isCleanupOver() {
		mf.unmap()
		mf.file.Close()
		logger.Infof("mapped file close success, %s.", mf.fileName)

		if err := os.Remove(mf.file.Name()); err != nil {
			logger.Errorf("message store delete file %s err: %s.", mf.file.Name(), err)
			return false
		}
	}

	return true
}

func (mf *mappedFile) shutdown(intervalForcibly int64) {
	if mf.available {
		mf.available = false
		mf.firstShutdownTimestamp = system.CurrentTimeMillis()
		mf.release()
	} else if mf.refCount > 0 { // 强制shutdown
		if (system.CurrentTimeMillis() - mf.firstShutdownTimestamp) >= intervalForcibly {
			mf.refCount = -1000 - mf.refCount
			mf.release()
		}
	}
}

func (mf *mappedFile) selectMappedBuffer(pos int64) *mappedBufferResult {
	if pos < mf.wrotePostion && pos >= 0 {
		if mf.hold() {
			size := mf.byteBuffer.writePos - int(pos)
			if mf.byteBuffer.writePos > len(mf.byteBuffer.mmapBuf) {
				return nil
			}

			newMmpBuffer := newMappedByteBuffer(mf.byteBuffer.Bytes())
			newMmpBuffer.writePos = mf.byteBuffer.writePos
			newMmpBuffer.readPos = int(pos)
			return newMappedBufferResult(mf.fileFromOffset+pos, newMmpBuffer, int32(size), mf)
		}
	}

	return nil
}

func (mf *mappedFile) selectMappedBufferByPosAndSize(pos int64, size int32) *mappedBufferResult {
	if (pos + int64(size)) <= mf.wrotePostion {
		if mf.hold() {
			end := pos + int64(size)
			if end > int64(len(mf.byteBuffer.mmapBuf)) {
				return nil
			}

			byteBuffer := newMappedByteBuffer(mf.byteBuffer.mmapBuf[pos:end])
			byteBuffer.writePos = int(size)
			return newMappedBufferResult(mf.fileFromOffset+pos, byteBuffer, size, mf)
		} else {
			logger.Warnf("matched, but hold failed, request pos: %d, fileFromOffset: %d.", pos, mf.fileFromOffset)
		}
	} else {
		logger.Warnf("select mapped buffer request pos invalid, request pos: %d, size: %d, fileFromOffset: %d.",
			pos, size, mf.fileFromOffset)
	}

	return nil
}

func (mf *mappedFile) cleanup(currentRef int64) bool {
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

	clean(mf.byteBuffer)
	// totalMappedVitualMemory
	// totalmappedFiles
	logger.Infof("unmap file[REF:%d] %s success.", currentRef, mf.fileName)

	return true
}

func (mf *mappedFile) release() {
	atomic.AddInt64(&mf.refCount, -1)
	if atomic.LoadInt64(&mf.refCount) > 0 {
		return
	}

	mf.rwLock.Lock()
	mf.cleanupOver = mf.cleanup(atomic.LoadInt64(&mf.refCount))
	mf.rwLock.Unlock()
}

// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
package stgstorelog

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/mmap"
	"github.com/toolkits/file"
)

const (
	OS_PAGE_SIZE       = 1024 * 4
	MMAPED_ENTIRE_FILE = -1
)

// maped_file 封装mapedfile类用于操作commitlog文件及consumelog文件
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
type MapedFile struct {
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
	mappedByteBuffer *MappedByteBuffer
	//mmapBytes        mmap.MemoryMap
	// 当前写到什么位置
	wrotePostion int64
	// Flush到什么位置
	committedPosition int64
	// 最后一条消息存储时间
	storeTimestamp     int64
	firstCreateInQueue bool
	// 文件读写锁
	rwLock *sync.RWMutex
}

// NewMapedFile 根据文件名新建mapedfile
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
func NewMapedFile(filePath string, filesize int64) (*MapedFile, error) {
	mapedFile := new(MapedFile)
	mapedFile.fileName = filePath
	mapedFile.fileSize = filesize
	mapedFile.rwLock = new(sync.RWMutex)

	commitRootDir := GetParentDirectory(filePath)
	logger.Info(commitRootDir)
	ensureDirOK(commitRootDir)

	exist, err := PathExists(filePath)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666)
	defer file.Close()
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}
	mapedFile.file = file

	if exist == false {
		// 如果文件不存在则新建filesize大小文件
		bytes := make([]byte, filesize)
		file.Write(bytes)
	}

	fileName := filepath.Base(mapedFile.file.Name())

	// 文件名即offset起始地址
	offset, err := strconv.ParseInt(fileName, 10, 64)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}
	mapedFile.fileFromOffset = offset

	mmapBytes, err := mmap.MapRegion(file, MMAPED_ENTIRE_FILE, mmap.RDWR, 0, 0)
	if err != nil {
		logger.Error(err.Error())
		//return nil, err
	}

	mapedFile.mappedByteBuffer = NewMappedByteBuffer(mmapBytes)
	atomic.AddInt64(&mapedFile.TotalMapedVitualMemory, int64(filesize))
	atomic.AddInt32(&mapedFile.TotalMapedFiles, 1)

	return mapedFile, nil
}

// AppendMessageWithCallBack 向MapedBuffer追加消息
// Return: appendNums 成功添加消息字节数
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
func (self *MapedFile) AppendMessageWithCallBack(msg interface{}, appendMessageCallback AppendMessageCallback) *AppendMessageResult {
	if msg == nil {
		panic(errors.New("AppendMessage nil msg error!!!"))
	}

	curPos := atomic.LoadInt64(&self.wrotePostion)
	// 表示还有剩余空间
	if curPos < self.fileSize {
		result := appendMessageCallback.doAppend(self.fileFromOffset, self.mappedByteBuffer, int32(self.fileSize)-int32(curPos), msg)
		atomic.AddInt64(&self.wrotePostion, int64(result.WroteBytes))
		self.storeTimestamp = result.StoreTimestamp
		return result
	}

	// TODO: 上层应用应该保证不会走到这里???
	logger.Errorf("MapedFile.appendMessage return null, wrotePostion:%d fileSize:%d", curPos, self.fileSize)
	return &AppendMessageResult{Status: APPENDMESSAGE_UNKNOWN_ERROR}
}

// appendMessage 向存储层追加数据，一般在SLAVE存储结构中使用
// Params: data 追加数据
// Return: 追加是否成功
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/6
func (self *MapedFile) appendMessage(data []byte) bool {
	currPos := self.wrotePostion
	if int64(currPos)+int64(len(data)) <= self.fileSize {
		n, err := self.mappedByteBuffer.Write(data)
		if err != nil {
			panic(err)
			return false
		}
		atomic.AddInt64(&self.wrotePostion, int64(n))
		return true
	} else {
		return false
	}
}

// Commit 消息提交刷盘
// Params: flushLeastPages 一次刷盘最少个数
// Return: flushPosition 当前刷盘位置
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/6
func (self *MapedFile) Commit(flushLeastPages int32) (flushPosition int64) {
	if self.isAbleToFlush(flushLeastPages) {
		// 对文件加写锁
		self.rwLock.Lock()
		// 获取当前写的位置
		currPos := self.wrotePostion
		// 将mappedByteBuffer的数据强制刷新到磁盘文件中
		self.Flush()
		//self.mmapBytes
		// 刷新完毕，则将committedPosition即flush的位置更新为当前位置记录
		self.committedPosition = currPos
		// 释放锁
		self.rwLock.Unlock()
	}
	return self.committedPosition
}

func (self *MapedFile) Flush() {
	self.mappedByteBuffer.flush()
}

func (self *MapedFile) Unmap() {
	atomic.AddInt64(&self.TotalMapedVitualMemory, -int64(self.fileSize))
	atomic.AddInt32(&self.TotalMapedFiles, -1)
	self.mappedByteBuffer.unmap()
}

// isAbleToFlush 根据最少需要刷盘page数值来判断当前是否需要立即刷新缓存数据到磁盘
// Params: flushLeastPages一次刷盘最少个数
// Return: 是否需要立即刷盘
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/6
func (self *MapedFile) isAbleToFlush(flushLeastPages int32) bool {
	// 获取当前flush到磁盘的位置
	flush := self.committedPosition
	// 获取当前write到缓冲区的位置
	write := self.wrotePostion
	if self.isFull() {

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

func (self *MapedFile) isFull() bool {
	return self.fileSize == int64(self.wrotePostion)
}

func (self *MapedFile) destroy() bool {
	// TODO: 次数没有使用this.shutdown(intervalForcibly)，是否有问题？？？
	self.Unmap()
	error := file.Remove(self.file.Name())
	if error != nil {
		logger.Error(error.Error())
		return false
	}
	return true
}

func (self *MapedFile) selectMapedBuffer(pos int64) *SelectMapedBufferResult {
	if pos < self.wrotePostion && pos >= 0 {
		logger.Info(string(self.mappedByteBuffer.MMapBuf))
		byteBuffer := self.mappedByteBuffer.slice()
		mmpBuffer := NewMappedByteBuffer(byteBuffer.Bytes())
		mmpBuffer.ReadPos = int(pos)
		size := mmpBuffer.WritePos - int(pos)
		newByteBuffer := mmpBuffer.slice()
		newMmpBuffer := NewMappedByteBuffer(newByteBuffer.Bytes())
		newMmpBuffer.Limit = size
		return NewSelectMapedBufferResult(self.fileFromOffset+pos, newMmpBuffer, int32(size), self)
	}

	return nil
}

func (self *MapedFile) selectMapedBufferByPosAndSize(pos int64, size int32) *SelectMapedBufferResult {
	if (pos + int64(size)) <= self.wrotePostion {
		byteBuffer := NewMappedByteBuffer(self.mappedByteBuffer.MMapBuf[:self.mappedByteBuffer.ReadPos])
		byteBufferNew := NewMappedByteBuffer(byteBuffer.MMapBuf[:pos])
		byteBufferNew.Limit = int(size)
		return &SelectMapedBufferResult{StartOffset: self.fileFromOffset + pos,
			MappedByteBuffer: byteBuffer, Size: size, MapedFile: self}
	} else {
		logger.Warnf("selectMapedBuffer request pos invalid, request pos: %d, size: %d, fileFromOffset: %d",
			pos, size, self.fileFromOffset)
	}

	return nil
}

func (self *MapedFile) release() {
	if self.committedPosition < self.wrotePostion {
		return
	}

	// TODO
}

func ensureDirOK(dirName string) error {
	if len(dirName) > 0 {
		exist, err := PathExists(dirName)

		if err != nil {
			logger.Info(err.Error())
			return err
		}

		if !exist {
			err := os.MkdirAll(dirName, os.ModePerm)
			if err != nil {
				logger.Info("crate store root dir %s, error:%s", dirName, err.Error())
				return err
			}
		}
	}

	return nil
}

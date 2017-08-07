// Copyright 2017 The Authors. All rights reserved.
// Use of this source code is governed by a Apache
// license that can be found in the LICENSE file.
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/5
package stgstorelog

import (
	"os"
	"strconv"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/mmap"
	"sync/atomic"
	"errors"
	"time"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"sync"
	"path"
)

const (
	OS_PAGE_SIZE       = 1024 * 4
	MMAPED_ENTIRE_FILE = -1
)

// maped_file 封装mapedfile类用于操作commitlog文件及consumelog文件
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/5
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
	fileSize int
	// 映射的文件
	file *os.File
	// 映射的内存对象，position永远不变
	mappedByteBuffer *MappedByteBuffer
	//mmapBytes        mmap.MemoryMap
	// 当前写到什么位置
	wrotePostion int32
	// Flush到什么位置
	committedPosition int32
	// 最后一条消息存储时间
	storeTimestamp     int64
	firstCreateInQueue bool
	// 文件读写锁
	rwLock *sync.RWMutex
}

// NewMapedFile 根据文件名新建mapedfile
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/5
func NewMapedFile(dir string, fileName string, filesize int) *MapedFile {
	mapedFile := &MapedFile{}
	mapedFile.fileName = fileName
	mapedFile.fileSize = filesize

	mapedFile.dirNotExistAndCreateDir(dir)

	file, err := os.OpenFile(path.Join(dir, fileName), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err.Error())
	}
	mapedFile.file = file
	bytes := make([]byte, filesize/2)
	file.Write(bytes)
	defer file.Close()

	// 文件名即offset起始地址
	offset, err := strconv.ParseInt(mapedFile.fileName, 10, 64)
	if err != nil {
		panic(err.Error())
	}
	mapedFile.fileFromOffset = offset

	mmapBytes, err := mmap.MapRegion(file, MMAPED_ENTIRE_FILE, mmap.RDWR, 0, 0)

	mapedFile.mappedByteBuffer = NewMappedByteBuffer(mmapBytes)
	atomic.AddInt64(&mapedFile.TotalMapedVitualMemory, int64(filesize))
	atomic.AddInt32(&mapedFile.TotalMapedFiles, 1)

	return mapedFile
}

// OpenMapedFile 根据文件名映射mapedfile
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/5
func OpenMapedFile(dir string, fileName string) *MapedFile {
	mapedFile := &MapedFile{}
	mapedFile.fileName = fileName

	file, err := os.OpenFile(path.Join(dir, fileName), os.O_RDWR, 0666)
	if err != nil {
		panic(err.Error())
	}
	filestat, error := file.Stat()
	if error != nil {
		panic(err.Error())
	}

	mapedFile.fileSize = int(filestat.Size())
	mapedFile.file = file
	defer file.Close()

	// 文件名即offset起始地址
	offset, err := strconv.ParseInt(mapedFile.fileName, 10, 64)
	if err != nil {
		panic(err.Error())
	}
	mapedFile.fileFromOffset = offset

	mmapBytes, err := mmap.MapRegion(file, MMAPED_ENTIRE_FILE, mmap.RDWR, 0, 0)

	mapedFile.mappedByteBuffer = NewMappedByteBuffer(mmapBytes)
	atomic.AddInt64(&mapedFile.TotalMapedVitualMemory, int64(mapedFile.fileSize))
	atomic.AddInt32(&mapedFile.TotalMapedFiles, 1)

	return mapedFile
}

// dirNotExistAndCreateDir 目录不存在则创建该目录
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/6
func (self *MapedFile) dirNotExistAndCreateDir(path string) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		err := os.MkdirAll(path, 0777)
		if err != nil {
			panic(err)
		}
	}
}

// AppendMessageWithCallBack 向MapedBuffer追加消息
// Return: appendNums 成功添加消息字节数
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/5
func (self *MapedFile) AppendMessageWithCallBack(msg interface{}, appendMessageCallback AppendMessageCallback) (appendNums int) {
	if msg == nil {
		panic(errors.New("AppendMessage nil msg error!!!"))
	}

	curPos := atomic.LoadInt32(&self.wrotePostion)
	// 表示还有剩余空间
	if int(curPos) < self.fileSize {
		appendNums := appendMessageCallback.doAppend(self.fileFromOffset, self.mappedByteBuffer, self.fileSize-int(curPos), msg)
		atomic.AddInt32(&self.wrotePostion, int32(appendNums))
		self.storeTimestamp = time.Now().UnixNano()
		return appendNums
	}

	// TODO: 上层应用应该保证不会走到这里???
	logger.Error("AppendMessage 上层应用应该保证不会走到这里!!!")
	return -1
}

// appendMessage 向存储层追加数据，一般在SLAVE存储结构中使用
// Params: data 追加数据
// Return: 追加是否成功
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/6
func (self *MapedFile) appendMessage(data []byte) bool {
	currPos := int(self.wrotePostion)
	if currPos+len(data) <= self.fileSize {
		n, err := self.mappedByteBuffer.Write(data)
		if err != nil {
			panic(err)
			return false
		}
		atomic.AddInt32(&self.wrotePostion, int32(n))
		return true
	} else {
		return false
	}
}

// Commit 消息提交刷盘
// Params: flushLeastPages 一次刷盘最少个数
// Return: flushPosition 当前刷盘位置
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/6
func (self *MapedFile) Commit(flushLeastPages int32) (flushPosition int32) {
	if self.isAbleToFlush(flushLeastPages) {
		// 对文件加写锁
		self.rwLock.Lock()
		// 获取当前写的位置
		currPos := self.wrotePostion
		// 将mappedByteBuffer的数据强制刷新到磁盘文件中
		self.mappedByteBuffer.Flush()
		//self.mmapBytes
		// 刷新完毕，则将committedPosition即flush的位置更新为当前位置记录
		self.committedPosition = currPos
		// 释放锁
		self.rwLock.Unlock()
	}
	return self.committedPosition
}

// isAbleToFlush 根据最少需要刷盘page数值来判断当前是否需要立即刷新缓存数据到磁盘
// Params: flushLeastPages一次刷盘最少个数
// Return: 是否需要立即刷盘
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/6
func (self *MapedFile) isAbleToFlush(flushLeastPages int32) (bool) {
	// 获取当前flush到磁盘的位置
	flush := self.committedPosition
	// 获取当前write到缓冲区的位置
	write := self.wrotePostion
	if self.isFull() {

		return true
	}
	// 只有未刷盘数据满足指定page数目才刷盘
	// OS_PAGE_SIZE默认为1024*4=4k
	if (flushLeastPages > 0) {
		// 计算出前期写缓冲区的位置到已刷盘的数据位置之间的数据，是否大于等于设置的至少得刷盘page个数
		// 超过则需要刷盘
		return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
	}

	// 如果flushLeastPages为0，那么则是每次有数据写入缓冲区则则直接刷盘
	return write > flush;
}

func (self *MapedFile) isFull() bool {
	return self.fileSize == int(self.wrotePostion)
}

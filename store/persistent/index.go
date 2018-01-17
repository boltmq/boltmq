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
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/boltmq/store"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/sysflag"
)

type files []os.FileInfo

func (fls files) Len() int {
	return len(fls)
}

func (fls files) Less(i, j int) bool {
	return fls[i].ModTime().Unix() < fls[j].ModTime().Unix()
}

func (fls files) Swap(i, j int) {
	fls[i], fls[j] = fls[j], fls[i]
}

type indexService struct {
	messageStore  *PersistentMessageStore
	hashSlotNum   int32
	indexNum      int32
	storePath     string
	indexFileList *list.List
	readWriteLock sync.RWMutex
	requestQueue  chan interface{}
	closeChan     chan bool
	stop          bool
}

func newIndexService(messageStore *PersistentMessageStore) *indexService {
	index := new(indexService)
	index.messageStore = messageStore
	index.hashSlotNum = messageStore.config.MaxHashSlotNum
	index.indexNum = messageStore.config.MaxIndexNum
	index.storePath = common.GetStorePathIndex(messageStore.config.StorePathRootDir)

	index.indexFileList = list.New()
	index.requestQueue = make(chan interface{}, 300000)

	return index
}

func (idx *indexService) load(lastExitOK bool) bool {
	fls, err := ioutil.ReadDir(idx.storePath)
	if err != nil {
		// TODO
	}

	if fls != nil {
		// ascending order
		sort.Sort(files(fls))
		for _, file := range fls {
			filePath := filepath.FromSlash(idx.storePath + string(os.PathSeparator) + file.Name())
			idxFile := newIndexFile(filePath, idx.hashSlotNum, idx.indexNum, int64(0), int64(0))
			idxFile.load()

			if !lastExitOK {
				// TODO
			}

			logger.Infof("load index file %s success.", filePath)
			idx.indexFileList.PushBack(idxFile)
		}
	}

	return true
}

func (idx *indexService) start() {
	logger.Info("index service started.")

	for {
		select {
		case request := <-idx.requestQueue:
			if request != nil {
				idx.buildIndex(request)
			}
		}
	}

	logger.Info("index service end.")
}

func (idx *indexService) buildIndex(request interface{}) {
	breakdown := false
	idxFile := idx.retryGetAndCreateIndexFile()
	if idxFile != nil {
		msg := request.(*dispatchRequest)
		if msg.commitLogOffset < idxFile.getEndPhyOffset() {
			return
		}

		tranType := sysflag.GetTransactionValue(int(msg.sysFlag))

		if sysflag.TransactionPreparedType == tranType {
			return
		}
		if sysflag.TransactionRollbackType == tranType {
			return
		}

		if len(msg.keys) > 0 {
			keySet := strings.Split(msg.keys, message.KEY_SEPARATOR)
			for _, key := range keySet {
				if len(key) > 0 {
					for ok := idxFile.putKey(idx.buildKey(msg.topic, key), msg.commitLogOffset, msg.storeTimestamp); !ok; {
						logger.Warnf("index file full, so create another one, %s.", idxFile.mf.fileName)

						idxFile = idx.retryGetAndCreateIndexFile()
						if idxFile == nil {
							breakdown = true
							return
						}
					}
				}
			}
		}
	} else {
		breakdown = true
	}

	if breakdown {
		logger.Error("build index error, stop building index.")
	}
}

func (idx *indexService) buildKey(topic, key string) string {
	return topic + "#" + key
}

func (idx *indexService) retryGetAndCreateIndexFile() *indexFile {
	var idxFile *indexFile

	// 如果创建失败，尝试重建3次
	for times := 0; times < 3; times++ {
		idxFile = idx.getAndCreateLastIndexFile()
		if idxFile != nil {
			break
		}

		time.Sleep(1000 * time.Millisecond)
	}

	if idxFile == nil {
		idx.messageStore.runFlags.makeIndexFileError()
		logger.Error("mark index file can not build flag.")
	}

	return idxFile
}

func (idx *indexService) getAndCreateLastIndexFile() *indexFile {
	var (
		idxFile                  *indexFile
		prevIndexFile            *indexFile
		lastUpdateEndPhyOffset   int64
		lastUpdateIndexTimestamp int64
	)

	idx.readWriteLock.RLock()
	if idx.indexFileList.Len() > 0 {
		tmp := idx.indexFileList.Back()
		indexTemp := tmp.Value.(*indexFile)
		if indexTemp != nil && !indexTemp.isWriteFull() {
			idxFile = indexTemp
		} else {
			lastUpdateEndPhyOffset = indexTemp.getEndPhyOffset()
			lastUpdateIndexTimestamp = indexTemp.getEndTimestamp()
			prevIndexFile = indexTemp
		}
	}
	idx.readWriteLock.RUnlock()

	// 如果没找到，使用写锁创建文件
	if idxFile == nil {
		fileName := fmt.Sprintf("%s%c%s", idx.storePath, os.PathSeparator, timeMillisecondToHumanString(time.Now()))
		idxFile := newIndexFile(fileName, idx.hashSlotNum, idx.indexNum, lastUpdateEndPhyOffset, lastUpdateIndexTimestamp)

		idx.readWriteLock.Lock()
		idx.indexFileList.PushBack(idxFile)
		idx.readWriteLock.Unlock()

		// 每创建一个新文件，之前文件要刷盘
		if idxFile != nil {
			flushThisFile := prevIndexFile
			go idx.flush(flushThisFile)
		}
	}

	return idxFile
}

func (idx *indexService) destroy() {
	idx.readWriteLock.RLock()
	defer idx.readWriteLock.RUnlock()

	for element := idx.indexFileList.Front(); element != nil; element = element.Next() {
		idxFile := element.Value.(*indexFile)
		idxFile.destroy(1000 * 3)
	}

	idx.indexFileList = list.New()
}

func (idx *indexService) deleteExpiredFile(offset int64) {
	idx.readWriteLock.RLock()
	defer idx.readWriteLock.RUnlock()

	fls := list.New()

	if idx.indexFileList.Len() > 0 {
		firstElement := idx.indexFileList.Front()
		firstIndexFile := firstElement.Value.(*indexFile)
		endPhyOffset := firstIndexFile.getEndPhyOffset()
		if endPhyOffset < offset {
			fls.PushBackList(idx.indexFileList)
		}
	}

	if fls.Len() > 0 {
		expiredFiles := list.New()
		for element := fls.Front(); element != nil; element = element.Next() {
			idxFile := element.Value.(*indexFile)
			if idxFile.getEndPhyOffset() < offset {
				expiredFiles.PushBack(idxFile)
			} else {
				break
			}
		}

		idx.deleteExpiredFiles(expiredFiles)
	}
}

func (idx *indexService) deleteExpiredFiles(fls *list.List) {
	if fls.Len() > 0 {
		idx.readWriteLock.Lock()
		defer idx.readWriteLock.Unlock()

		for e := fls.Front(); e != nil; e = e.Next() {
			expiredFile := e.Value.(*indexFile)
			destroyed := expiredFile.destroy(3000)
			if !destroyed {
				logger.Errorf("delete expired file destroy failed, %s.", expiredFile.mf.fileName)
				break
			}

			idx.indexFileList.Remove(e)
		}
	}
}

func (idx *indexService) queryOffset(topic, key string, maxNum int32, begin, end int64) *store.QueryMessageResult {
	// TODO
	return nil
}

func (idx *indexService) putRequest(request interface{}) {
	// TODO
	idx.requestQueue <- request
}

func (idx *indexService) flush(idxFile *indexFile) {
	if nil == idxFile {
		return
	}

	var indexMsgTimestamp int64

	if idxFile.isWriteFull() {
		indexMsgTimestamp = idxFile.getEndTimestamp()
	}

	idxFile.flush()

	if indexMsgTimestamp > 0 {
		idx.messageStore.steCheckpoint.indexMsgTimestamp = indexMsgTimestamp

	}
}

func (idx *indexService) shutdown() {
	// TODO
}

var (
	HASH_SLOT_SIZE int32 = 4
	INDEX_SIZE     int32 = 20
	INVALID_INDEX  int32 = 0
)

type indexFile struct {
	hashSlotNum int32
	indexNum    int32
	mf          *mappedFile
	byteBuffer  *mappedByteBuffer
	header      *indexHeader
}

func newIndexFile(fileName string, hashSlotNum, indexNum int32, endPhyOffset, endTimestamp int64) *indexFile {
	fileTotalSize := INDEX_HEADER_SIZE + (hashSlotNum * HASH_SLOT_SIZE) + (indexNum * INDEX_SIZE)

	idxFile := new(indexFile)
	mf, err := newMappedFile(fileName, int64(fileTotalSize))
	if err != nil {
		// TODO
	}

	idxFile.mf = mf
	idxFile.byteBuffer = idxFile.mf.byteBuffer
	idxFile.hashSlotNum = hashSlotNum
	idxFile.indexNum = indexNum

	idxFile.header = newIndexHeader(newMappedByteBuffer(make([]byte, idxFile.mf.fileSize)))

	if endPhyOffset > 0 {
		idxFile.header.setBeginPhyOffset(endPhyOffset)
		idxFile.header.setEndPhyOffset(endPhyOffset)
	}

	if endTimestamp > 0 {
		idxFile.header.setBeginTimestamp(endTimestamp)
		idxFile.header.setEndTimestamp(endTimestamp)
	}

	return idxFile
}

func (idxFile *indexFile) load() {
	idxFile.header.load()
}

func (idxFile *indexFile) flush() {
	beginTime := time.Now().UnixNano() / 1000000
	if idxFile.mf.hold() {
		idxFile.header.updateByteBuffer()
		idxFile.byteBuffer.flush()
		idxFile.mf.release()
		endTime := time.Now().UnixNano() / 1000000
		logger.Infof("flush index file eclipse time(ms) %d.", endTime-beginTime)
	}
}

func (idxFile *indexFile) isWriteFull() bool {
	return idxFile.header.indexCount >= idxFile.indexNum
}

func (idxFile *indexFile) getEndPhyOffset() int64 {
	return idxFile.header.endPhyOffset
}

func (idxFile *indexFile) getEndTimestamp() int64 {
	return idxFile.header.endTimestamp
}

func (idxFile *indexFile) putKey(key string, phyOffset int64, storeTimestamp int64) bool {
	if idxFile.header.indexCount < idxFile.indexNum {
		keyHash := idxFile.indexKeyHashMethod(key)
		slotPos := keyHash % idxFile.hashSlotNum
		absSlotPos := INDEX_HEADER_SIZE + slotPos*HASH_SLOT_SIZE

		idxFile.byteBuffer.readPos = int(absSlotPos)
		slotValue := idxFile.byteBuffer.ReadInt32()
		if slotValue <= INVALID_INDEX || slotValue > idxFile.header.indexCount {
			slotValue = INVALID_INDEX
		}

		timeDiff := storeTimestamp - idxFile.header.beginTimestamp
		// 时间差存储单位由毫秒改为秒
		timeDiff = timeDiff / 1000

		if idxFile.header.beginTimestamp <= 0 {
			timeDiff = 0
		} else if timeDiff > 0x7fffffff {
			timeDiff = 0x7fffffff
		} else if timeDiff < 0 {
			timeDiff = 0
		}

		absIndexPos := INDEX_HEADER_SIZE + idxFile.hashSlotNum*HASH_SLOT_SIZE + idxFile.header.indexCount*INDEX_SIZE

		// 写入真正索引
		idxFile.byteBuffer.writePos = int(absIndexPos)
		idxFile.byteBuffer.WriteInt32(keyHash)
		idxFile.byteBuffer.WriteInt64(phyOffset)
		idxFile.byteBuffer.WriteInt32(int32(timeDiff))
		idxFile.byteBuffer.WriteInt32(slotValue)

		// 更新哈希槽
		currentwritePos := idxFile.byteBuffer.writePos
		idxFile.byteBuffer.writePos = int(absIndexPos)
		idxFile.byteBuffer.WriteInt32(idxFile.header.indexCount)
		idxFile.byteBuffer.writePos = currentwritePos

		// 第一次写入
		if idxFile.header.indexCount <= 1 {
			idxFile.header.beginPhyOffset = phyOffset
			idxFile.header.beginTimestamp = storeTimestamp
		}

		idxFile.header.incHashSlotCount()
		idxFile.header.incIndexCount()
		idxFile.header.endPhyOffset = phyOffset
		idxFile.header.endTimestamp = storeTimestamp

		return true
	}

	return false
}

func (idxFile *indexFile) indexKeyHashMethod(key string) int32 {
	keyHash := idxFile.indexKeyHashCode(key)
	keyHashPositive := math.Abs(float64(keyHash))
	if keyHashPositive < 0 {
		keyHashPositive = 0
	}
	return int32(keyHashPositive)
}

func (idxFile *indexFile) indexKeyHashCode(key string) int32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int32(h.Sum32())
}

func (idxFile *indexFile) destroy(intervalForcibly int64) bool {
	return idxFile.mf.destroy(intervalForcibly)
}

var (
	INDEX_HEADER_SIZE    int32 = 40
	BEGINTIMESTAMP_INDEX int32 = 0
	ENDTIMESTAMP_INDEX   int32 = 8
	BEGINPHYOFFSET_INDEX int32 = 16
	ENDPHYOFFSET_INDEX   int32 = 24
	HASHSLOTCOUNT_INDEX  int32 = 32
	INDEXCOUNT_INDEX     int32 = 36
)

type indexHeader struct {
	byteBuffer     *mappedByteBuffer
	beginTimestamp int64
	endTimestamp   int64
	beginPhyOffset int64
	endPhyOffset   int64
	hashSlotCount  int32
	indexCount     int32
}

func newIndexHeader(byteBuffer *mappedByteBuffer) *indexHeader {
	indexHeader := new(indexHeader)
	indexHeader.byteBuffer = byteBuffer
	return indexHeader
}

func (header *indexHeader) load() {
	header.byteBuffer.readPos = int(BEGINTIMESTAMP_INDEX)
	atomic.StoreInt64(&header.beginTimestamp, header.byteBuffer.ReadInt64())
	atomic.StoreInt64(&header.endTimestamp, header.byteBuffer.ReadInt64())
	atomic.StoreInt64(&header.beginPhyOffset, header.byteBuffer.ReadInt64())
	atomic.StoreInt64(&header.endPhyOffset, header.byteBuffer.ReadInt64())
	atomic.StoreInt32(&header.hashSlotCount, header.byteBuffer.ReadInt32())
	atomic.StoreInt32(&header.indexCount, header.byteBuffer.ReadInt32())

	if header.indexCount <= 0 {
		atomic.AddInt32(&header.indexCount, 1)
	}
}

func (header *indexHeader) updateByteBuffer() {
	header.byteBuffer.writePos = int(BEGINTIMESTAMP_INDEX)
	header.byteBuffer.WriteInt64(header.beginTimestamp)
	header.byteBuffer.WriteInt64(header.endTimestamp)
	header.byteBuffer.WriteInt64(header.beginPhyOffset)
	header.byteBuffer.WriteInt64(header.endPhyOffset)
	header.byteBuffer.WriteInt32(header.hashSlotCount)
	header.byteBuffer.WriteInt32(header.indexCount)
}

func (header *indexHeader) setBeginTimestamp(beginTimestamp int64) {
	header.beginTimestamp = beginTimestamp
	header.byteBuffer.WriteInt64(beginTimestamp)
}

func (header *indexHeader) setEndTimestamp(endTimestamp int64) {
	header.endTimestamp = endTimestamp
	header.byteBuffer.WriteInt64(endTimestamp)
}

func (header *indexHeader) setBeginPhyOffset(beginPhyOffset int64) {
	header.beginPhyOffset = beginPhyOffset
	header.byteBuffer.WriteInt64(beginPhyOffset)
}

func (header *indexHeader) setEndPhyOffset(endPhyOffset int64) {
	header.endPhyOffset = endPhyOffset
	header.byteBuffer.WriteInt64(endPhyOffset)
}

func (header *indexHeader) incHashSlotCount() {
	value := atomic.AddInt32(&header.hashSlotCount, int32(1))
	header.byteBuffer.WriteInt32(value)
}

func (header *indexHeader) incIndexCount() {
	value := atomic.AddInt32(&header.indexCount, int32(1))
	header.byteBuffer.WriteInt32(value)
}

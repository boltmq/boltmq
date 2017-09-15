package stgstorelog

import (
	"container/list"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
)

type Files []os.FileInfo

func (self Files) Len() int {
	return len(self)
}

func (self Files) Less(i, j int) bool {
	return self[i].ModTime().Unix() < self[j].ModTime().Unix()
}

func (self Files) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

type IndexService struct {
	defaultMessageStore *DefaultMessageStore
	hashSlotNum         int32
	indexNum            int32
	storePath           string
	indexFileList       *list.List
	readWriteLock       sync.RWMutex
	requestQueue        chan interface{}
	closeChan           chan bool
	stop                bool
}

func NewIndexService(messageStore *DefaultMessageStore) *IndexService {
	service := new(IndexService)
	service.defaultMessageStore = messageStore
	service.hashSlotNum = messageStore.MessageStoreConfig.MaxHashSlotNum
	service.indexNum = messageStore.MessageStoreConfig.MaxIndexNum
	service.storePath = config.GetStorePathIndex(messageStore.MessageStoreConfig.StorePathRootDir)

	service.indexFileList = list.New()
	service.requestQueue = make(chan interface{}, 300000)

	return service
}

func (self *IndexService) Load(lastExitOK bool) bool {
	files, err := ioutil.ReadDir(self.storePath)
	if err != nil {
		// TODO
	}

	if files != nil {
		// ascending order
		sort.Sort(Files(files))
		for _, file := range files {
			filePath := filepath.FromSlash(self.storePath + string(os.PathSeparator) + file.Name())
			indexFile := NewIndexFile(filePath, self.hashSlotNum, self.indexNum, int64(0), int64(0))
			indexFile.load()

			if !lastExitOK {
				// TODO
			}

			logger.Info("load index file OK, %s", filePath)
			self.indexFileList.PushBack(indexFile)
		}
	}

	return true
}

func (self *IndexService) Start() {
	for {
		select {
		case request := <-self.requestQueue:
			if request != nil {
				self.buildIndex(request)
			}
		}
	}
}

func (self *IndexService) buildIndex(request interface{}) {
	// TODO indexFie := self.retryGetAndCreateIndexFile()
}

func (self *IndexService) retryGetAndCreateIndexFile() *IndexFile {
	var indexFile *IndexFile

	// 如果创建失败，尝试重建3次
	for times := 0; times < 3; times++ {
		indexFile := self.getAndCreateLastIndexFile()
		if indexFile != nil {
			break
		}

		time.Sleep(1000 * time.Millisecond)
	}

	if indexFile == nil {
		self.defaultMessageStore.RunningFlags.makeIndexFileError()
		logger.Error("mark index file can not build flag")
	}

	return indexFile
}

func (self *IndexService) getAndCreateLastIndexFile() *IndexFile {
	var (
		indexFile                *IndexFile
		prevIndexFile            *IndexFile
		lastUpdateEndPhyOffset   int64
		lastUpdateIndexTimestamp int64
	)

	self.readWriteLock.RLock()
	if self.indexFileList.Len() > 0 {
		tmp := self.indexFileList.Back()
		indexTemp := tmp.Value.(*IndexFile)
		if !indexTemp.isWriteFull() {
			indexFile = indexTemp
		} else {
			lastUpdateEndPhyOffset = indexTemp.getEndPhyOffset()
			lastUpdateIndexTimestamp = indexTemp.getEndTimestamp()
			prevIndexFile = indexTemp
		}
	}
	self.readWriteLock.RUnlock()

	// 如果没找到，使用写锁创建文件
	if indexFile == nil {
		fileName := self.storePath + GetPathSeparator() + utils.TimeMillisecondToHumanString(time.Now())
		indexFile := NewIndexFile(fileName, self.hashSlotNum, self.indexNum, lastUpdateEndPhyOffset, lastUpdateIndexTimestamp)

		self.readWriteLock.Lock()
		self.indexFileList.PushBack(indexFile)
		self.readWriteLock.Unlock()

		// 每创建一个新文件，之前文件要刷盘
		if indexFile != nil {
			flushThisFile := prevIndexFile
			go self.flush(flushThisFile)
		}
	}

	return indexFile
}

func (self *IndexService) queryOffset(topic, key string, maxNum int32, begin, end int64) *QueryMessageResult {
	// TODO
	return nil
}

func (self *IndexService) putRequest(request interface{}) {
	// TODO
	self.requestQueue <- request
}

func (self *IndexService) flush(indexFile *IndexFile) {
	if nil == indexFile {
		return
	}

	var indexMsgTimestamp int64

	if indexFile.isWriteFull() {
		indexMsgTimestamp = indexFile.getEndTimestamp()
	}

	indexFile.flush()

	if indexMsgTimestamp > 0 {
		self.defaultMessageStore.StoreCheckpoint.indexMsgTimestamp = indexMsgTimestamp
		
	}
}

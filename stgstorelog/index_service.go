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
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"strings"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
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
	readWriteLock       *sync.RWMutex
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
	service.readWriteLock = new(sync.RWMutex)

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

			logger.Infof("load index file OK, %s", filePath)
			self.indexFileList.PushBack(indexFile)
		}
	}

	return true
}

func (self *IndexService) Start() {
	logger.Info("index service started")

	for {
		select {
		case request := <-self.requestQueue:
			if request != nil {
				self.buildIndex(request)
			}
		}
	}

	logger.Info("index service end")
}

func (self *IndexService) buildIndex(request interface{}) {
	breakdown := false
	indexFile := self.retryGetAndCreateIndexFile()
	if indexFile != nil {
		msg := request.(*DispatchRequest)
		if msg.commitLogOffset < indexFile.getEndPhyOffset() {
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
					for ok := indexFile.putKey(self.buildKey(msg.topic, key), msg.commitLogOffset, msg.storeTimestamp); !ok; {
						logger.Warn("index file full, so create another one, ", indexFile.mapedFile.fileName)

						indexFile = self.retryGetAndCreateIndexFile()
						if indexFile == nil {
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
		logger.Error("build index error, stop building index")
	}
}

func (self *IndexService) buildKey(topic, key string) string {
	return topic + "#" + key
}

func (self *IndexService) retryGetAndCreateIndexFile() *IndexFile {
	var indexFile *IndexFile

	// 如果创建失败，尝试重建3次
	for times := 0; times < 3; times++ {
		indexFile = self.getAndCreateLastIndexFile()
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
		if indexTemp != nil && !indexTemp.isWriteFull() {
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

func (self *IndexService) destroy() {
	self.readWriteLock.RLock()
	defer self.readWriteLock.RUnlock()

	for element := self.indexFileList.Front(); element != nil; element = element.Next() {
		indexFile := element.Value.(*IndexFile)
		indexFile.destroy(1000 * 3)
	}

	self.indexFileList = list.New()
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

func (self *IndexService) Shutdown() {
	// TODO
}

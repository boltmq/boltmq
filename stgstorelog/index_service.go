package stgstorelog

import (
	"container/list"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
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

func (self *IndexService) load(lastExitOK bool) bool {
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

func (self *IndexService) run() {

}

func (self *IndexService) queryOffset(topic, key string, maxNum int32, begin, end int64) *QueryMessageResult {
	// TODO
	return nil
}

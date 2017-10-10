package process

import (
	baseStore "git.oschina.net/cloudzone/smartgo/stgclient/consumer/store"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	set "github.com/deckarep/golang-set"
	"github.com/pquerna/ffjson/ffjson"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

// LocalFileOffsetStore: 本地存储，针对广播消息
// Author: yintongqiang
// Since:  2017/8/28

type LocalFileOffsetStore struct {
	mQClientFactory *MQClientInstance
	// 消费名称
	groupName string
	// 保存每个队列的offset
	offsetTable map[string]baseStore.MessageQueueExt
	sync.RWMutex
	// 本地存储路径
	storePath string
}

func NewLocalFileOffsetStore(mQClientFactory *MQClientInstance, groupName string) *LocalFileOffsetStore {
	path := os.Getenv("smartgo.client.localOffsetStoreDir")
	if strings.EqualFold(path, "") {
		path = stgcommon.GetUserHomeDir() + string(os.PathSeparator) + ".smartgo_offsets"
	}
	path = path + string(os.PathSeparator) + mQClientFactory.ClientId + string(os.PathSeparator) +
		groupName + string(os.PathSeparator) + "offsets.json"
	return &LocalFileOffsetStore{mQClientFactory: mQClientFactory, groupName: groupName,
		offsetTable: make(map[string]baseStore.MessageQueueExt), storePath: strings.Replace(path, "\\", "/", -1)}
}

// 读取本地offset
func (store *LocalFileOffsetStore) Load() {
	offsetSerializeWrapper := store.readLocalOffset()
	if offsetSerializeWrapper != nil {
		for mqStr, mqExt := range offsetSerializeWrapper.OffsetTable {
			store.Lock()
			store.offsetTable[mqStr] = mqExt
			store.Unlock()
			logger.Infof("load consumer's offset, %v %v %v", store.groupName, mqStr, mqExt.Offset)
		}
	}
}

// 更新本地offset
func (store *LocalFileOffsetStore) UpdateOffset(mq *message.MessageQueue, offset int64, increaseOnly bool) {
	defer store.Unlock()
	if mq != nil {
		store.Lock()
		mqOffsetOld, ok := store.offsetTable[mq.Key()]
		if !ok {
			mqOffsetExt := baseStore.MessageQueueExt{MessageQueue: *mq, Offset: offset}
			store.offsetTable[mq.Key()] = mqOffsetExt
			mqOffsetOld = mqOffsetExt
		} else {
			if increaseOnly {
				stgcommon.CompareAndIncreaseOnly(&mqOffsetOld.Offset, offset)
			} else {
				store.offsetTable[mq.Key()] = mqOffsetOld
			}
		}
	}
}

// 读取本地offset
func (store *LocalFileOffsetStore) readLocalOffset() *baseStore.OffsetSerializeWrapper {
	bytes, err := ioutil.ReadFile(store.storePath)
	var offsetSerializeWrapper = baseStore.NewOffsetSerializeWrapper()
	if err != nil && len(bytes) == 0 {
		return store.readLocalOffsetBak()
	} else {
		err = ffjson.Unmarshal(bytes, offsetSerializeWrapper)
		if err != nil {
			return store.readLocalOffsetBak()
		}
		return offsetSerializeWrapper
	}
}

// 读取本地offset的备份
func (store *LocalFileOffsetStore) readLocalOffsetBak() *baseStore.OffsetSerializeWrapper {
	bytes, err := ioutil.ReadFile(store.storePath + ".bak")
	var offsetSerializeWrapper = baseStore.NewOffsetSerializeWrapper()
	if err != nil && len(bytes) == 0 {
		logger.Errorf("ReadFile: ", err.Error())
		return nil
	} else {
		err = ffjson.Unmarshal(bytes, offsetSerializeWrapper)
		if err != nil {
			logger.Errorf("Unmarshal: ", err.Error())
			return nil
		}
		return offsetSerializeWrapper
	}
}

// 读取本地offset的备份
func (store *LocalFileOffsetStore) ReadOffset(mq *message.MessageQueue, rType baseStore.ReadOffsetType) int64 {
	if mq != nil {
		switch rType {
		case baseStore.MEMORY_FIRST_THEN_STORE:
		case baseStore.READ_FROM_MEMORY:
			mqOffsetExt, ok := store.offsetTable[mq.Key()]
			if ok {
				return mqOffsetExt.Offset
			} else if baseStore.READ_FROM_MEMORY == rType {
				return -1
			}

		case baseStore.READ_FROM_STORE:
			wrapper := store.readLocalOffset()
			if wrapper == nil {
				return -1
			}
			mqOffsetExt, ok := wrapper.OffsetTable[mq.Key()]
			if ok {
				store.UpdateOffset(mq, mqOffsetExt.Offset, false)
				return mqOffsetExt.Offset
			}

		default:

		}
	}
	return -1
}

// 持久化
func (store *LocalFileOffsetStore) Persist(mq *message.MessageQueue) {

}

// 删除offset
func (store *LocalFileOffsetStore) RemoveOffset(mq *message.MessageQueue) {
}

// 持久化所有队列
func (store *LocalFileOffsetStore) PersistAll(mqs set.Set) {
	if mqs == nil || len(mqs.ToSlice()) == 0 {
		return
	}
	offsetSerializeWrapper := baseStore.NewOffsetSerializeWrapper()
	for mqKey, offset := range store.offsetTable {
		//if mqs.Contains(mq) {
		if store.setContainsMQ(mqKey, mqs) {
			offsetSerializeWrapper.Lock()
			offsetSerializeWrapper.OffsetTable[mqKey] = offset
			offsetSerializeWrapper.Unlock()
		}
	}
	data, err := ffjson.Marshal(offsetSerializeWrapper)
	if err == nil {
		stgcommon.String2File(data, store.storePath)
	} else {
		logger.Errorf("offsetSerializeWrapper Marshal error=%v ", err.Error())
	}
}
func (store *LocalFileOffsetStore) setContainsMQ(mqKey string, mqSet set.Set) bool {
	containsFlag := false
	for mqs := range mqSet.Iterator().C {
		ms := mqs.(*message.MessageQueue)
		if strings.EqualFold(ms.Key(), mqKey) {
			containsFlag = true
			break
		}
	}
	return containsFlag
}

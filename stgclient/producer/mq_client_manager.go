package producer

import (
	"sync"
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"sync/atomic"
)

var (
	once sync.Once
	instance *MQClientManager
)
// MQClientInstance管理类
// Author: yintongqiang
// Since:  2017/8/8

func GetInstance() *MQClientManager {
	if instance == nil {
		once.Do(func() {
			instance = NewMQClientManager()
		})
	}
	return instance
}

type MQClientManager struct {
	FactoryTable          map[string]*MQClientInstance
	FactoryIndexGenerator int32
}

func NewMQClientManager() *MQClientManager {
	return &MQClientManager{FactoryTable:make(map[string]*MQClientInstance)}
}

func (mQClientManager *MQClientManager) GetAndCreateMQClientInstance(clientConfig *stgclient.ClientConfig) *MQClientInstance {
	clientId := clientConfig.BuildMQClientId()
	instance := mQClientManager.FactoryTable[clientId]
	if (nil == instance) {
		instance = NewMQClientInstance(clientConfig.CloneClientConfig(), atomic.AddInt32(&mQClientManager.FactoryIndexGenerator, 1), clientId);
		prev := mQClientManager.FactoryTable[clientId]
		if (prev != nil) {
			instance = prev;
		} else {
			mQClientManager.FactoryTable[clientId] = instance
		}
	}
	return instance
}

package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	set "github.com/deckarep/golang-set"
)

// RemoteBrokerOffsetStore: 保存offset到broker
// Author: yintongqiang
// Since:  2017/8/11

type RemoteBrokerOffsetStore struct {
	mQClientFactory *MQClientInstance
	groupName       string
}

func NewRemoteBrokerOffsetStore(mQClientFactory *MQClientInstance, groupName string) *RemoteBrokerOffsetStore {
	return &RemoteBrokerOffsetStore{mQClientFactory:mQClientFactory, groupName:groupName}
}

func (store *RemoteBrokerOffsetStore)Load() {

}

func (store *RemoteBrokerOffsetStore)PersistAll(mqs set.Set) {

}

func (store *RemoteBrokerOffsetStore)Persist(mq message.MessageQueue) {

}


package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
)
// PullAPIWrapper: pull包装类
// Author: yintongqiang
// Since:  2017/8/11

type PullAPIWrapper struct {
	pullFromWhichNodeTable *sync.Map
	mQClientFactory        *MQClientInstance
	consumerGroup          string
	unitMode               bool
	connectBrokerByUser    bool
	defaultBrokerId        int
}

func NewPullAPIWrapper(mQClientFactory *MQClientInstance, consumerGroup string, unitMode bool) *PullAPIWrapper {
	return &PullAPIWrapper{
		pullFromWhichNodeTable:sync.NewMap(),
		mQClientFactory:mQClientFactory,
		consumerGroup:consumerGroup,
		unitMode:unitMode,
		defaultBrokerId:stgcommon.MASTER_ID }
}

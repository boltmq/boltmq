package process

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)

type PullResultExt struct {
	*consumer.PullResult
	suggestWhichBrokerId int64
	messageBinary        [] byte
}

func NewPullResultExt(pullStatus consumer.PullStatus, nextBeginOffset int64, minOffset int64, maxOffset int64,
msgFoundList []message.MessageExt, suggestWhichBrokerId int64, messageBinary [] byte) *PullResultExt {
	pullResult := &consumer.PullResult{PullStatus:pullStatus, NextBeginOffset:nextBeginOffset,
		MinOffset:minOffset, MaxOffset:maxOffset, MsgFoundList:msgFoundList}
	return &PullResultExt{PullResult:pullResult, suggestWhichBrokerId:suggestWhichBrokerId, messageBinary:messageBinary}
}
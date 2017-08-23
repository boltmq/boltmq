package consumer

import "git.oschina.net/cloudzone/smartgo/stgcommon/message"

type PullResult struct {
	PullStatus      PullStatus
	NextBeginOffset int64
	MinOffset       int64
	MaxOffset       int64
	MsgFoundList    []message.MessageExt
}

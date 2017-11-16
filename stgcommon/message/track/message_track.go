package track

import (
	"fmt"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
)

type TrackTypes int

const (
	SubscribedAndConsumed       TrackTypes = iota // 已订阅并已消费(Offset越过了)
	SubscribedButFilterd                          // 已订阅，但消息被过滤掉了
	SubscribedButPull                             // 已订阅，但PULL拉取中，消费结果未知
	SubscribedAndNotConsumeYet                    // 已订阅，未消费(Offset较小)
	UnknowExeption                                // 触发未知异常
	NotSubscribedAndNotConsumed                   // 未订阅，未消费
	ConsumerGroupIdNotOnline                      // 消费组不在线(未消费)
)

type MessageTrack struct {
	ConsumerGroupId string     `json:"consumerGroup"` // 消费组
	TrackType       TrackTypes `json:"trackType"`     // 消息轨迹类型
	ExceptionDesc   string     `json:"exceptionDesc"` // 消费组描述
	Code            int        `json:"code"`          // 0:查询成功, 非0:查询异常
}

func NewMessageTrack(consumerGroupId string) *MessageTrack {
	messageTrack := &MessageTrack{
		ConsumerGroupId: consumerGroupId,
		TrackType:       UnknowExeption,
		Code:            code.SYSTEM_ERROR,
	}
	return messageTrack
}

func (track *MessageTrack) ToString() string {
	if track == nil {
		return "MessageTrack is nil"
	}

	trackDesc := fmt.Sprintf("%d(%s)", int(track.TrackType), ParseTrackDesc(int(track.TrackType)))
	format := "MessageTrack {consumerGroupId=%s, code=%d, %s, exceptionDesc=%s }"
	return fmt.Sprintf(format, track.ConsumerGroupId, track.Code, trackDesc, track.ExceptionDesc)
}

func ParseTrackDesc(trackType int) string {
	if desc, ok := patternTrackTypes[trackType]; ok {
		return desc
	}
	return ""
}

var patternTrackTypes = map[int]string{
	0: "SubscribedAndConsumed",
	1: "SubscribedButFilterd",
	2: "SubscribedButPull",
	3: "SubscribedAndNotConsumeYet",
	4: "UnknowExeption",
}

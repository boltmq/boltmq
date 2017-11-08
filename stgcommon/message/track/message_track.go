package track

import (
	"fmt"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
)

type TrackTypes int

const (
	SubscribedAndConsumed      TrackTypes = iota // 订阅了，而且消费了（Offset越过了）
	SubscribedButFilterd                         // 订阅了，但是被过滤掉了
	SubscribedButPull                            // 订阅了，但是PULL，结果未知
	SubscribedAndNotConsumeYet                   // 订阅了，但是没有消费（Offset小）
	UnknowExeption                               // 未知异常
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

package admin

import (
	"fmt"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
)

var TrackTypes = struct {
	SubscribedAndConsumed      int // 订阅了，而且消费了（Offset越过了）
	SubscribedButFilterd       int // 订阅了，但是被过滤掉了
	SubscribedButPull          int // 订阅了，但是PULL，结果未知
	SubscribedAndNotConsumeYet int // 订阅了，但是没有消费（Offset小）
	UnknowExeption             int // 未知异常
}{
	0,
	1,
	2,
	3,
	4,
}

var patternTrackTypes = map[int]string{
	0: "SubscribedAndConsumed",
	1: "SubscribedButFilterd",
	2: "SubscribedButPull",
	3: "SubscribedAndNotConsumeYet",
	4: "UnknowExeption",
}

type MessageTrack struct {
	ConsumerGroupId string `json:"consumerGroup"` // 消费组
	TrackType       int    `json:"trackType"`     // 消息轨迹类型
	ExceptionDesc   string `json:"exceptionDesc"` // 消费组描述
	Code            int    `json:"code"`          // 0:查询成功, 非0:查询异常
}

func NewMessageTrack(consumerGroupId string) *MessageTrack {
	messageTrack := &MessageTrack{
		ConsumerGroupId: consumerGroupId,
		TrackType:       TrackTypes.UnknowExeption,
		Code:            code.SYSTEM_ERROR,
	}
	return messageTrack
}

func (track *MessageTrack) ToString() string {
	if track == nil {
		return "MessageTrack is nil"
	}

	trackDesc := fmt.Sprintf("%d(%s)", track.TrackType, convertTrackDesc(track.TrackType))
	format := "MessageTrack {consumerGroupId=%s, code=%d, %s, exceptionDesc=%s }"
	return fmt.Sprintf(format, track.ConsumerGroupId, track.Code, trackDesc, track.ExceptionDesc)
}

func convertTrackDesc(trackType int) string {
	if desc, ok := patternTrackTypes[trackType]; ok {
		return desc
	}
	return ""
}

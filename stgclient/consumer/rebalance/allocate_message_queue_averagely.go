package rebalance

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"strings"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
)


// AllocateMessageQueueAveragely：平均负载
// Author: yintongqiang
// Since:  2017/8/8

type AllocateMessageQueueAveragely struct {

}

func (strategy AllocateMessageQueueAveragely) Allocate(consumerGroup string, currentCID string, mqAll [] *message.MessageQueue, cidAll [] string) [] *message.MessageQueue {
	if strings.EqualFold(currentCID, "") {
		panic("currentCID is empty")
	}
	if len(mqAll) == 0 {
		panic("mqAll is null or mqAll empty")
	}
	if len(cidAll) == 0 {
		panic("cidAll is null or cidAll empty")
	}
	var contains bool = false
	var index int
	for in, cid := range cidAll {
		if strings.EqualFold(cid, currentCID) {
			index = in
			contains = true
			break
		}
	}
	result := []*message.MessageQueue{}
	if !contains {
		logger.Warnf("[BUG] ConsumerGroup: %v The consumerId: %v not in cidAll: %v", consumerGroup, currentCID, cidAll)
		return result
	}
	mod := len(mqAll) % len(cidAll)
	var averageSize int = 1
	if len(mqAll) <= len(cidAll) {
		averageSize = 1
	} else {
		if mod > 0 && index < mod {
			averageSize = len(mqAll) / len(cidAll) + 1
		} else {
			averageSize = len(mqAll) / len(cidAll)
		}
	}
	var startIndex int
	if mod > 0 && index < mod {
		startIndex = index * averageSize
	} else {
		startIndex = index * averageSize + mod
	}
	rangeV := averageSize
	if len(mqAll) - startIndex < averageSize {
		rangeV = len(mqAll) - startIndex
	}
	for i := 0; i < rangeV; i++ {
		result = append(result, mqAll[(startIndex + i) % len(mqAll)])
	}
	return result
}

func (strategy AllocateMessageQueueAveragely) GetName() string {
	return "AVG"
}

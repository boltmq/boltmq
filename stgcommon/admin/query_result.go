package admin

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"strings"
)

type QueryResult struct {
	IndexLastUpdateTimestamp int64
	MessageList              []*message.MessageExt
}

func NewQueryResult(indexLastUpdateTimestamp int64, messageList []*message.MessageExt) *QueryResult {
	queryResult := &QueryResult{
		IndexLastUpdateTimestamp: indexLastUpdateTimestamp,
		MessageList:              messageList,
	}
	return queryResult
}

func (self *QueryResult) ToString() string {
	if self == nil {
		return "QueryResult is nil"
	}

	msgInfos := make([]string, 0)
	if self.MessageList != nil && len(self.MessageList) > 0 {
		for _, msg := range self.MessageList {
			msgInfos = append(msgInfos, msg.ToString())
		}
	}
	format := "QueryResult {IndexLastUpdateTimestamp=%d, MessageList=%s }"
	return fmt.Sprintf(format, self.IndexLastUpdateTimestamp, strings.Join(msgInfos, ","))
}

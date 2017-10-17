package body

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

type ConsumeMessageDirectlyResult struct {
	Order          bool
	AutoCommit     bool
	ConsumeResult  CMResult
	Remark         string
	SpentTimeMills int64
	*protocol.RemotingSerializable
}

func (self *ConsumeMessageDirectlyResult) ToString() string {
	if self == nil {
		return ""
	}
	format := "ConsumeMessageDirectlyResult {Order=%t, AutoCommit=%t, ConsumeResult=%s, SpentTimeMills=%d, Remark=%s}"
	return fmt.Sprintf(format, self.Order, self.AutoCommit, self.ConsumeResult.ToString(), self.SpentTimeMills, self.Remark)
}

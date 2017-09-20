package body

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	set "github.com/deckarep/golang-set"
)

type QueryConsumeTimeSpanBody struct {
	ConsumeTimeSpanSet set.Set `json:"consumeTimeSpanSet"`
	*protocol.RemotingSerializable
}

package body

import (
	set "github.com/deckarep/golang-set"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

type GroupList struct {
	GroupList set.Set `json:"groupList"`
	*protocol.RemotingSerializable
}

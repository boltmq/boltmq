package body

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

type KVTable struct {
	Table map[string]string `json:"table"`
	*protocol.RemotingSerializable
}

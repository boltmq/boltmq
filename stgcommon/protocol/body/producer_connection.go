package body

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	set "github.com/deckarep/golang-set"
)

type ProducerConnection struct {
	ConnectionSet set.Set `json:"connectionSet"`
	*protocol.RemotingSerializable
}

package remoting

import (
	"bytes"

	"github.com/boltmq/boltmq/net/core"
)

type PacketFragmentationAssembler interface {
	Pack(sa core.SocketAddr, buffer []byte) (bufs []*bytes.Buffer, e error)
}

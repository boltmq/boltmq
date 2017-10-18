package remoting

import "bytes"

type PacketFragmentationAssembler interface {
	Pack(addr string, buffer []byte) (bufs []*bytes.Buffer, e error)
}

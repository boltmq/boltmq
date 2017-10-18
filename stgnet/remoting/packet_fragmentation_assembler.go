package remoting

import "bytes"

type PacketFragmentationAssembler interface {
	UnPack(addr string, buffer []byte) (bufs []*bytes.Buffer, e error)
}

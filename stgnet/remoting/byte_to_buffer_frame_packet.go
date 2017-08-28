package remoting

import "bytes"

type ByteToBufferFramePacket interface {
	UnPack(addr string, buffer []byte) (bufs []*bytes.Buffer, e error)
}

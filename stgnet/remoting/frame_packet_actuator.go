package remoting

import "bytes"

type FramePacketActuator interface {
	UnPack(addr string, buffer []byte) (bufs []*bytes.Buffer, e error)
}

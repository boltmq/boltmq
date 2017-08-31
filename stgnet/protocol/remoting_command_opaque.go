package protocol

import "sync/atomic"

var (
	opaqueId int32
)

func inrcOpaque() int32 {
	return atomic.AddInt32(&opaqueId, 1)
}

func resetOpaque() {
	opaqueId = 0
}

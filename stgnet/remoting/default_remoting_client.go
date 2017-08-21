package remoting

import "sync"

type DefalutRemotingClient struct {
	responseTable      map[int32]*ResponseFuture
	responseTableLock  sync.RWMutex
	namesrvAddrList    []string
	namesrvAddrChoosed string
	isClosed           bool
}

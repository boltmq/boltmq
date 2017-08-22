package remoting

import "sync"

// DefalutRemotingClient default remoting client
type DefalutRemotingClient struct {
	responseTable      map[int32]*ResponseFuture
	responseTableLock  sync.RWMutex
	namesrvAddrList    []string
	namesrvAddrChoosed string
	isClosed           bool
}

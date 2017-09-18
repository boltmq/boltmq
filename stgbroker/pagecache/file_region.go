package pagecache

import "net"

type FileRegion interface {
	Position() int64
	Transfered() int64
	Count() int64
	TransferTo(var1 net.Conn, var2 int64)
	ReferenceCounted
}

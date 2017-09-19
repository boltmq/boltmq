package pagecache

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

type FileRegion interface {
	Position() int64
	Transfered() int64
	Count() int64
	TransferTo(ctx netm.Context, var2 int64)
	ReferenceCounted
}

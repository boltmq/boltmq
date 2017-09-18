package pagecache

type ReferenceCounted interface {
	RefCnt() int64

	Retain() ReferenceCounted

	Retain2(var1 int32) ReferenceCounted

	Release() bool

	Release2(var1 int32) bool
}

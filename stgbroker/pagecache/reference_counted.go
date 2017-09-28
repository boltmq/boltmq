package pagecache

type ReferenceCounted interface {
	RefCnt() int64

	Retain() ReferenceCounted

	RetainInt(var1 int32) ReferenceCounted

	Release() bool

	ReleaseInt(var1 int32) bool
}

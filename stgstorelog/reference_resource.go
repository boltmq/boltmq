package stgstorelog

type ReferenceResource struct {
	refCount               int32
	available              bool
	cleanupOver            bool
	firstShutdownTimestamp int32
}

func NewReferenceResource() *ReferenceResource {
	return &ReferenceResource{
		refCount:    int32(1),
		available:   true,
		cleanupOver: false,
	}
}

func (self *ReferenceResource) hold() bool {
	if self.available {
		// TODO
	}
	return false
}

package stgstorelog

import (
	"sync"
	"sync/atomic"
)

type CleanReferenceResource interface {
	cleanup(currentRef int64) bool
}

type ReferenceResource struct {
	CleanReferenceResource
	refCount               int64
	available              bool
	cleanupOver            bool
	firstShutdownTimestamp int64
	mutexs                 *sync.Mutex
}

func NewReferenceResource() *ReferenceResource {
	return &ReferenceResource{
		refCount:               int64(1),
		available:              true,
		cleanupOver:            false,
		firstShutdownTimestamp: 0,
		mutexs:                 new(sync.Mutex),
	}
}

func (self *ReferenceResource) hold() bool {
	self.mutexs.Lock()
	defer self.mutexs.Unlock()

	if self.available {
		atomic.AddInt64(&self.refCount, 1)
		if atomic.LoadInt64(&self.refCount) > 0 {
			return true
		} else {
			atomic.AddInt64(&self.refCount, -1)
		}
	}

	return false
}

func (self *ReferenceResource) isAvailable() bool {
	return self.available
}

func (self *ReferenceResource) release() {
	atomic.AddInt64(&self.refCount, -1)
	value := atomic.LoadInt64(&self.refCount)
	if value > 0 {
		return
	}

	self.mutexs.Lock()
	self.cleanupOver = self.cleanup(value) // cleanup内部要对是否clean做处理
	self.mutexs.Unlock()
}

func (self *ReferenceResource) isCleanupOver() bool {
	return self.refCount <= 0 && self.cleanupOver
}

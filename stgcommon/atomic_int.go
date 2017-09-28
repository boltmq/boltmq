package stgcommon

import "sync/atomic"

type AtomicInt64 struct {
	value int64
}

func NewAtomicIn64(initialValue int) *AtomicInt64 {
	return &AtomicInt64{value: int64(initialValue)}
}

func (self *AtomicInt64) Get() int64 {
	return atomic.LoadInt64(&self.value)
}

func (self *AtomicInt64) Set(value int) {
	atomic.StoreInt64(&self.value, int64(value))
}

func (self *AtomicInt64) GetAndIncrement() int64 {
	oldValue := atomic.LoadInt64(&self.value)
	atomic.AddInt64(&self.value, 1)
	return oldValue
}

func (self *AtomicInt64) GetAndDecrement() int64 {
	oldValue := atomic.LoadInt64(&self.value)
	atomic.AddInt64(&self.value, -1)
	return atomic.LoadInt64(&oldValue)
}

func (self *AtomicInt64) GetAndAdd(value int) int64 {
	oldValue := atomic.LoadInt64(&self.value)
	atomic.AddInt64(&self.value, int64(value))
	return oldValue
}

func (self *AtomicInt64) IncrementAndGet() int64 {
	atomic.AddInt64(&self.value, 1)
	return atomic.LoadInt64(&self.value)
}

func (self *AtomicInt64) DecrementAndGet() int64 {
	atomic.AddInt64(&self.value, -1)
	return atomic.LoadInt64(&self.value)
}

func (self *AtomicInt64) AddAndGet(value int) int64 {
	atomic.AddInt64(&self.value, int64(value))
	return atomic.LoadInt64(&self.value)
}

package sync

import (
	"sync"
	"time"
)

// Cond 条件变量，同sync.Cond，增加WaitTimeout
type Cond struct {
	cond *sync.Cond
	done chan struct{}
}

// NewCond returns a new Cond.
func NewCond() *Cond {
	return &Cond{
		cond: sync.NewCond(new(sync.Mutex)),
		done: make(chan struct{}),
	}
}

// Wait cannot return unless awoken by Broadcast or Signal.
func (c *Cond) Wait() {
	c.cond.L.Lock()
	c.cond.Wait()
	c.cond.L.Unlock()
}

// WaitTimeout cannot return unless awoken by Broadcast or Signal, timeout.
func (c *Cond) WaitTimeout(timeout time.Duration) {
	go func() {
		c.cond.L.Lock()
		c.cond.Wait()
		c.cond.L.Unlock()
		c.done <- struct{}{}
		//close(c.done)
	}()

	select {
	case <-time.After(timeout):
	case <-c.done:
	}
}

// Signal wakes one goroutine waiting on c, if there is any.
func (c *Cond) Signal() {
	c.cond.Signal()
}

// Broadcast wakes all goroutines waiting on c.
func (c *Cond) Broadcast() {
	c.cond.Broadcast()
}

// Close close done chan
func (c *Cond) Close() {
	if c.done != nil {
		close(c.done)
		c.done = nil
	}
}

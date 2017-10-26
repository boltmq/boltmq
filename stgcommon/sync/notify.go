package sync

import (
	"time"
)

// Notify 唤醒Goroutine
type Notify struct {
	num  int64
	done chan struct{}
}

// NewNotify create notfiy
func NewNotify() *Notify {
	return &Notify{
		done: make(chan struct{}),
	}
}

// Wait cannot return unless awoken by Signal.
func (n *Notify) Wait() {
	n.num++
	<-n.done
	n.num--
}

// WaitTimeout cannot return unless awoken by Signal or timeout.
func (n *Notify) WaitTimeout(timeout time.Duration) {
	n.num++
	select {
	case <-time.After(timeout):
	case <-n.done:
	}
	n.num--
}

// Signal wakes one goroutine waiting on c, if there is any.
func (n *Notify) Signal() {
	if n.num > 0 {
		n.done <- struct{}{}
	}
}

// Close close c
func (n *Notify) Close() {
	if n.done != nil {
		close(n.done)
		n.done = nil
	}
}

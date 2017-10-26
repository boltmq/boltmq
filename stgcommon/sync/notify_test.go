package sync

import (
	"sync"
	"testing"
	"time"
)

func TestNotifyWait(t *testing.T) {
	var (
		wg sync.WaitGroup
		c  int
		w  = 2
	)

	n := NewNotify()

	wg.Add(1)
	go func() {
		for i := 0; i < w; i++ {
			n.Wait()
			c++
		}
		wg.Done()
	}()

	time.Sleep(10 * time.Millisecond)
	for i := 0; i < w; i++ {
		n.Signal()
	}
	wg.Wait()

	if c != w {
		t.Errorf("Notify Wait: not hold")
	}
}

func TestNotifyWaitTimeout(t *testing.T) {
	var (
		wg sync.WaitGroup
		c  int
		w  = 2
	)

	n := NewNotify()

	wg.Add(1)
	go func() {
		for i := 0; i < w; i++ {
			n.WaitTimeout(2 * time.Millisecond)
			c++
		}
		wg.Done()
	}()

	time.Sleep(10 * time.Millisecond)
	for i := 0; i < w; i++ {
		n.Signal()
	}
	wg.Wait()

	if c != w {
		t.Errorf("Notify Wait: not hold")
	}
}

func TestNotifyWaitTimeout2(t *testing.T) {
	var (
		wg sync.WaitGroup
		c  int
		w  = 2
	)

	n := NewNotify()

	wg.Add(1)
	go func() {
		for i := 0; i < w; i++ {
			n.WaitTimeout(20 * time.Millisecond)
			c++
		}
		wg.Done()
	}()

	time.Sleep(10 * time.Millisecond)
	for i := 0; i < w; i++ {
		n.Signal()
	}
	wg.Wait()

	if c != w {
		t.Errorf("Notify Wait: not hold")
	}
}

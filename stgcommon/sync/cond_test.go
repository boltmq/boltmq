package sync

import (
	"sync"
	"testing"
	"time"
)

func TestCondWait(t *testing.T) {
	var (
		wg sync.WaitGroup
		n  int
		w  = 2
	)

	c := NewCond()

	wg.Add(1)
	go func() {
		for i := 0; i < w; i++ {
			c.Wait()
			n++
		}
		wg.Done()
	}()

	for i := 0; i < w; i++ {
		time.Sleep(1 * time.Millisecond)
		c.Signal()
	}
	wg.Wait()
	//c.Close()

	if n != w {
		t.Errorf("Cond Wait: not hold")
	}
}

func TestCondWaitTimeout(t *testing.T) {
	var (
		wg sync.WaitGroup
		n  int
		w  = 2
	)

	c := NewCond()

	wg.Add(1)
	go func() {
		for i := 0; i < w; i++ {
			c.WaitTimeout(5 * time.Millisecond)
			n++
		}
		wg.Done()
	}()

	for i := 0; i < w; i++ {
		time.Sleep(10 * time.Millisecond)
		c.Signal()
	}
	wg.Wait()
	//c.Close()

	if n != w {
		t.Errorf("Cond Wait: not hold")
	}
}

func TestCondWaitTimeout2(t *testing.T) {
	var (
		wg sync.WaitGroup
		n  int
		w  = 2
	)

	c := NewCond()

	wg.Add(1)
	go func() {
		for i := 0; i < w; i++ {
			c.WaitTimeout(5 * time.Millisecond)
			n++
		}
		wg.Done()
	}()

	for i := 0; i < w; i++ {
		time.Sleep(1 * time.Millisecond)
		c.Signal()
	}
	wg.Wait()
	//c.Close()

	if n != w {
		t.Errorf("Cond Wait: not hold")
	}
}

func TestCondWaitBroadcast(t *testing.T) {
	var (
		wg sync.WaitGroup
		n  int
		w  = 2
	)

	c := NewCond()

	wg.Add(1)
	go func() {
		c.Wait()
		n++
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		c.Wait()
		n++
		wg.Done()
	}()

	time.Sleep(10 * time.Millisecond)
	c.Broadcast()
	wg.Wait()
	//c.Close()

	if n != w {
		t.Errorf("Cond Wait: not hold")
	}
}

func TestCondWaitTimeoutBroadcast(t *testing.T) {
	var (
		wg sync.WaitGroup
		n  int
		w  = 2
	)

	c := NewCond()

	wg.Add(1)
	go func() {
		for i := 0; i < w; i++ {
			c.WaitTimeout(20 * time.Millisecond)
			n++
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := 0; i < w; i++ {
			c.WaitTimeout(2 * time.Millisecond)
			n++
		}
		wg.Done()
	}()

	time.Sleep(10 * time.Millisecond)
	c.Broadcast()
	wg.Wait()
	//c.Close()

	if n != w*2 {
		t.Errorf("Cond Wait: not hold")
	}
}

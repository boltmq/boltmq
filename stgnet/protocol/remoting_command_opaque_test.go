package protocol

import (
	"sync"
	"testing"
)

func TestInrcOpaque(t *testing.T) {
	var (
		i int32
	)

	resetOpaque()

	i = inrcOpaque()
	if i != 1 {
		t.Errorf("Test failed, %d incorrect, except[%d]", i, 1)
		return
	}
	i = inrcOpaque()
	if i != 2 {
		t.Errorf("Test failed, %d incorrect, except[%d]", i, 2)
		return
	}
	i = inrcOpaque()
	if i != 3 {
		t.Errorf("Test failed, %d incorrect, except[%d]", i, 3)
		return
	}
}

func TestThreadInrcOpaque(t *testing.T) {
	var (
		wg sync.WaitGroup
		o  int32
		c  = 3
	)

	resetOpaque()

	wg.Add(c)
	fn := func() {
		o = inrcOpaque()
		wg.Done()
	}

	for i := 0; i < c; i++ {
		go fn()
	}

	wg.Wait()
	if o != 3 {
		t.Errorf("Test failed, %d incorrect, except[%d]", o, 3)
		return
	}
}

package timeutil

import (
	"testing"
	"time"
)

func TestTicker(t *testing.T) {
	var i int

	tk := NewTicker(true, -1, 100*time.Millisecond, func() {
		i++
	})
	tk.Start()

	time.Sleep(800 * time.Millisecond)
	ok := tk.Stop()
	if !ok {
		t.Error("ticker stop failed")
	}

	if i != 8 {
		t.Error("ticker error")
	}
}

func TestTickerNotWait(t *testing.T) {
	var i int

	tk := NewTicker(false, -1, 100*time.Millisecond, func() {
		i++
	})
	tk.Start()

	time.Sleep(850 * time.Millisecond)
	ok := tk.Stop()
	if !ok {
		t.Error("ticker stop failed")
	}

	if i != 8 {
		t.Error("ticker error")
	}
}

func TestTickerNoDelay(t *testing.T) {
	var i int

	tk := NewTicker(true, 0, 100*time.Millisecond, func() {
		i++
	})
	tk.Start()

	time.Sleep(800 * time.Millisecond)
	ok := tk.Stop()
	if !ok {
		t.Error("ticker stop failed")
	}

	if i != 9 {
		t.Error("ticker error")
	}
}

func TestTickerNoDelayNotWait(t *testing.T) {
	var i int

	tk := NewTicker(false, 0, 100*time.Millisecond, func() {
		i++
	})
	tk.Start()

	time.Sleep(850 * time.Millisecond)
	ok := tk.Stop()
	if !ok {
		t.Error("ticker stop failed")
	}

	if i != 9 {
		t.Error("ticker error")
	}
}

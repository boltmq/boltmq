package timeutil

import (
	"testing"
	"time"
)

func TestTickersClose(t *testing.T) {
	var i int

	tk := NewTicker(true, 0, 100*time.Millisecond, func() {
		i++
	})
	ts := NewTickers()
	ts.Register("flush", tk)

	time.Sleep(800 * time.Millisecond)
	e := ts.Close()
	if e != nil {
		t.Error("tickers close: %v", e)
	}

	if i != 8 {
		t.Error("tickers error")
	}

}

func TestTickersRemove(t *testing.T) {
	var i int

	tk := NewTicker(true, 0, 100*time.Millisecond, func() {
		i++
	})
	ts := NewTickers()
	ts.Register("flush", tk)

	time.Sleep(800 * time.Millisecond)
	e := ts.Remove("flush")
	if e != nil {
		t.Error("tickers remove: %v", e)
	}

	if i != 8 {
		t.Error("tickers error")
	}
}

func TestTickersDelay(t *testing.T) {
	var i int

	tk := NewTicker(true, time.Second, 300*time.Millisecond, func() {
		time.Sleep(200 * time.Millisecond)
		i++
	})
	ts := NewTickers()
	ts.Register("flush", tk)

	time.Sleep(400 * time.Millisecond)
	e := ts.Close()
	if e != nil {
		t.Errorf("tickers close: %v", e)
	}

	if i != 2 {
		t.Errorf("tickers error: %d", i)
	}
}

func TestTickersWait(t *testing.T) {
	var i int

	tk := NewTicker(true, 0, 300*time.Millisecond, func() {
		time.Sleep(200 * time.Millisecond)
		i++
	})
	ts := NewTickers()
	ts.Register("flush", tk)

	time.Sleep(400 * time.Millisecond)
	e := ts.Close()
	if e != nil {
		t.Errorf("tickers close: %v", e)
	}

	if i != 1 {
		t.Errorf("tickers error: %d", i)
	}
}

func TestTickersNotWait(t *testing.T) {
	var i int

	tk := NewTicker(false, 0, 300*time.Millisecond, func() {
		time.Sleep(200 * time.Millisecond)
		i++
	})
	ts := NewTickers()
	ts.Register("flush", tk)

	time.Sleep(400 * time.Millisecond)
	e := ts.Close()
	if e != nil {
		t.Errorf("tickers close: %v", e)
	}

	if i != 0 {
		t.Errorf("tickers error: %d", i)
	}
}

func TestTickersMutil(t *testing.T) {
	var i int
	var j int
	var k int

	tk := NewTicker(true, 0, 300*time.Millisecond, func() {
		time.Sleep(200 * time.Millisecond)
		i++
	})

	tk2 := NewTicker(true, 0, 400*time.Millisecond, func() {
		time.Sleep(300 * time.Millisecond)
		j++
	})

	tk3 := NewTicker(true, 0, 1500*time.Millisecond, func() {
		time.Sleep(1300 * time.Millisecond)
		k++
	})

	ts := NewTickers()
	ts.Register("flush", tk)
	ts.Register("flush2", tk2)
	ts.Register("flush3", tk3)

	time.Sleep(1800 * time.Millisecond)
	e := ts.Close()
	if e != nil {
		t.Errorf("tickers close: %v", e)
	}

	if i != 4 {
		t.Errorf("tickers error: %d", i)
	}

	if j != 3 {
		t.Errorf("tickers error: %d", j)
	}

	if k != 1 {
		t.Errorf("tickers error: %d", k)
	}
}

func TestTickersMutilNotWait(t *testing.T) {
	var i int
	var j int
	var k int

	tk := NewTicker(false, 0, 300*time.Millisecond, func() {
		time.Sleep(200 * time.Millisecond)
		i++
	})

	tk2 := NewTicker(false, 0, 400*time.Millisecond, func() {
		time.Sleep(300 * time.Millisecond)
		j++
	})

	tk3 := NewTicker(false, 0, 1500*time.Millisecond, func() {
		time.Sleep(1300 * time.Millisecond)
		k++
	})

	ts := NewTickers()
	ts.Register("flush", tk)
	ts.Register("flush2", tk2)
	ts.Register("flush3", tk3)

	time.Sleep(1800 * time.Millisecond)
	e := ts.Close()
	if e != nil {
		t.Errorf("tickers close: %v", e)
	}

	if i != 3 {
		t.Errorf("tickers error: %d", i)
	}

	if j != 2 {
		t.Errorf("tickers error: %d", j)
	}

	if k != 0 {
		t.Errorf("tickers error: %d", k)
	}
}

package timeutil

import (
	"testing"
	"time"
)

func TestTickersClose(t *testing.T) {
	var i int

	ts := NewTickers()
	ts.Register("flush", true, 0, 100*time.Millisecond, func() {
		i++
	})

	time.Sleep(850 * time.Millisecond)
	if i != 8 {
		t.Error("tickers error")
	}

	e := ts.Close()
	if e != nil {
		t.Error("tickers close: %v", e)
	}
}

func TestTickersRemove(t *testing.T) {
	var i int

	ts := NewTickers()
	ts.Register("flush", true, 0, 100*time.Millisecond, func() {
		i++
	})

	time.Sleep(850 * time.Millisecond)
	if i != 8 {
		t.Error("tickers error")
	}

	e := ts.Remove("flush")
	if e != nil {
		t.Error("tickers remove: %v", e)
	}
}

func TestTickersDelay(t *testing.T) {
	var i int

	ts := NewTickers()
	ts.Register("flush", true, time.Second, 300*time.Millisecond, func() {
		time.Sleep(200 * time.Millisecond)
		i++
	})

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

	ts := NewTickers()
	ts.Register("flush", true, 0, 300*time.Millisecond, func() {
		time.Sleep(200 * time.Millisecond)
		i++
	})

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

	ts := NewTickers()
	ts.Register("flush", false, 0, 300*time.Millisecond, func() {
		time.Sleep(200 * time.Millisecond)
		i++
	})

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

	ts := NewTickers()
	ts.Register("flush", true, 0, 300*time.Millisecond, func() {
		time.Sleep(200 * time.Millisecond)
		i++
	})

	ts.Register("flush2", true, 0, 400*time.Millisecond, func() {
		time.Sleep(300 * time.Millisecond)
		j++
	})

	ts.Register("flush3", true, 0, 1500*time.Millisecond, func() {
		time.Sleep(1300 * time.Millisecond)
		k++
	})

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

	ts := NewTickers()
	ts.Register("flush", false, 0, 300*time.Millisecond, func() {
		time.Sleep(200 * time.Millisecond)
		i++
	})

	ts.Register("flush2", false, 0, 400*time.Millisecond, func() {
		time.Sleep(300 * time.Millisecond)
		j++
	})

	ts.Register("flush3", false, 0, 1500*time.Millisecond, func() {
		time.Sleep(1300 * time.Millisecond)
		k++
	})

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

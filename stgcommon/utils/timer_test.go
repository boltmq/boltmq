package utils

import (
	"testing"
	"time"
)

func TestNewTicker(t *testing.T) {
	ticker := NewTicker(1, 1)
	if ticker == nil {
		t.Error("NewTicker is faild")
		return
	}

	//t.Log("NewMap success")
}

func TestTickerDo(t *testing.T) {
	var (
		ticker = NewTicker(1, 1)
		i      int
	)

	go ticker.Do(func(tm time.Time) {
		i++
	})

	time.Sleep(time.Second * 4)
	ticker.Stop()

	if i != 3 {
		t.Error("ticker error")
	}
}

func TestNewChannelTicker(t *testing.T) {
	ticker := NewChannelTicker(1, 1)
	if ticker == nil {
		t.Error("NewChannelTicker is faild")
		return
	}

	//t.Log("NewMap success")
}

func TestChannelTickerDo(t *testing.T) {
	var (
		ticker = NewChannelTicker(1, 1)
		i      int
	)

	go ticker.Do(func(tm time.Time) {
		i++
	})

	time.Sleep(time.Second * 4)
	ticker.Stop()

	if i != 3 {
		t.Error("channel ticker error")
	}
}

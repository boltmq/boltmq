package timeutil

import (
	"sync"

	"github.com/facebookgo/errgroup"
	"github.com/go-errors/errors"
)

// Tickers 定时器管理
type Tickers struct {
	lock    sync.RWMutex
	tickers map[string]*Ticker
}

// NewTickers 创建定时器管理
func NewTickers() *Tickers {
	return &Tickers{
		tickers: make(map[string]*Ticker),
	}
}

// Register 注册一个定时器
func (ts *Tickers) Register(key string, t *Ticker) error {
	if t == nil {
		return errors.Errorf("ticker not nil.")
	}

	ts.lock.RLock()
	_, ok := ts.tickers[key]
	ts.lock.RUnlock()
	if ok {
		return errors.Errorf("ticker[%s] register already.", key)
	}

	ts.lock.Lock()
	ts.tickers[key] = t
	ts.lock.Unlock()

	return nil
}

// Remove 移除定时器
func (ts *Tickers) Remove(key string) error {
	ts.lock.RLock()
	t, ok := ts.tickers[key]
	ts.lock.RUnlock()
	if !ok {
		return errors.Errorf("ticker[%s] remove already.", key)
	}

	ts.lock.Lock()
	delete(ts.tickers, key)
	ts.lock.Unlock()

	// 停止定时器
	ok = t.Stop()
	if !ok {
		return errors.Errorf("ticker[%s] stop faild.", key)
	}

	return nil
}

// Get 取得定时器
func (ts *Tickers) Get(key string) *Ticker {
	ts.lock.RLock()
	t, ok := ts.tickers[key]
	ts.lock.RUnlock()
	if !ok {
		return nil
	}

	return t
}

//Start 开启所有定时器
func (ts *Tickers) Start() {
	ts.lock.RLock()
	for _, t := range ts.tickers {
		//启动定时器
		t.Start()
	}
	ts.lock.RUnlock()
}

//Close 关闭定时器
func (ts *Tickers) Close() error {
	var (
		length  int
		g       errgroup.Group
		wg      sync.WaitGroup
		tickers map[string]*Ticker
	)

	length = len(ts.tickers)
	tickers = make(map[string]*Ticker, length)
	wg.Add(length)

	ts.lock.Lock()
	for k, t := range ts.tickers {
		tickers[k] = t
		delete(ts.tickers, k)
	}
	ts.lock.Unlock()

	// 同时停止所有任务
	for k, t := range tickers {
		go func(t *Ticker) {
			ok := t.Stop()
			if !ok {
				g.Error(errors.Errorf("ticker[%s] stop faild.", k))
			}
			wg.Done()
		}(t)
	}

	wg.Wait()
	return g.Wait()
}

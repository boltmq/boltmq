package timeutil

import (
	"sync"
	"time"

	"github.com/facebookgo/errgroup"
	"github.com/go-errors/errors"
)

// Tickers 定时器管理
type Tickers struct {
	lock    sync.RWMutex
	tickers map[string]*ticker
}

// NewTickers 创建定时器管理
func NewTickers() *Tickers {
	return &Tickers{
		tickers: make(map[string]*ticker),
	}
}

type ticker struct {
	tm    *time.Timer
	d     time.Duration
	delay time.Duration
	fn    func()
	isRun bool
	wait  bool
	over  chan interface{}
}

func newTicker(wait bool, delay, d time.Duration, fn func()) *ticker {
	return &ticker{
		d:     d,
		delay: delay,
		fn:    fn,
		wait:  wait,
	}
}

func (t *ticker) run() {
	if t.wait {
		t.over = make(chan interface{})
	}

	t.tm = time.NewTimer(t.d)
	t.isRun = true

	if t.delay > 0 {
		time.Sleep(t.delay)
		t.fn()
	}

	for {
		select {
		case <-t.tm.C:
			t.fn()
			if !t.isRun {
				if t.over != nil {
					close(t.over)
				}
				return
			}

			t.tm.Reset(t.d)
		}
	}
}

func (t *ticker) flush() {
	if t.isRun {
		t.tm.Reset(t.d)
	}
}

func (t *ticker) stop() bool {
	if t.isRun == false {
		return true
	}

	// 等待正在执行的任务完成
	t.isRun = false
	if t.over != nil {
		<-t.over
	}
	return true
}

// Register 注册一个定时器
func (ts *Tickers) Register(key string, wait bool, delay, d time.Duration, f func()) error {
	if f == nil {
		return errors.Errorf("func not nil.")
	}

	ts.lock.RLock()
	_, ok := ts.tickers[key]
	ts.lock.RUnlock()
	if ok {
		return errors.Errorf("ticker[%s] register already.", key)
	}

	t := newTicker(wait, delay, d, f)
	ts.lock.Lock()
	ts.tickers[key] = t
	ts.lock.Unlock()

	//启动定时器
	ts.startTicker(t)

	return nil
}

func (ts *Tickers) startTicker(t *ticker) {
	go t.run()
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
	ok = t.stop()
	if !ok {
		return errors.Errorf("ticker[%s] stop faild.", key)
	}

	return nil
}

//Close 关闭定时器
func (ts *Tickers) Close() error {
	var (
		length  int
		g       errgroup.Group
		wg      sync.WaitGroup
		tickers map[string]*ticker
	)

	length = len(ts.tickers)
	tickers = make(map[string]*ticker, length)
	wg.Add(length)

	ts.lock.Lock()
	for k, t := range ts.tickers {
		tickers[k] = t
		delete(ts.tickers, k)
	}
	ts.lock.Unlock()

	// 同时停止所有任务
	for k, t := range tickers {
		go func(t *ticker) {
			ok := t.stop()
			if !ok {
				g.Error(errors.Errorf("ticker[%s] stop faild.", k))
			}
			wg.Done()
		}(t)
	}

	wg.Wait()
	return g.Wait()
}

package timeutil

import "time"

type Ticker struct {
	tm    *time.Timer
	d     time.Duration
	delay time.Duration
	fn    func()
	isRun bool
	wait  bool
	over  chan interface{}
}

func NewTicker(wait bool, delay, d time.Duration, fn func()) *Ticker {
	return &Ticker{
		d:     d,
		delay: delay,
		fn:    fn,
		wait:  wait,
	}
}

func (t *Ticker) Start() {
	if t.isRun {
		return
	}

	go t.start()
}

func (t *Ticker) start() {
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

func (t *Ticker) flush() {
	if t.isRun {
		t.tm.Reset(t.d)
	}
}

// Stop stop ticker
func (t *Ticker) Stop() bool {
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

package utils

import "time"

// Ticker ticker定时器
type Ticker struct {
	delay    int
	interval int
	timer    *time.Timer
}

// NewTicker 创建ticker定时器
func NewTicker(interval, delay int) *Ticker {
	return &Ticker{
		delay:    delay,
		interval: interval,
		timer:    time.NewTimer(time.Duration(interval) * time.Second),
	}
}

// Do ticker定时器，delay：延迟执行时间，interval：时间间隔，dofn：定时执行函数
// 此方法时间间隔interval + dofn执行时间
func (t *Ticker) Do(dofn func(time.Time)) {
	if t.delay > 0 {
		time.Sleep(time.Duration(t.delay) * time.Second)
	}

	for {
		select {
		case tc := <-t.timer.C:
			dofn(tc)
			t.timer.Reset(time.Duration(t.interval) * time.Second)
		}
	}
}

// Stop 停止ticker定时器
func (t *Ticker) Stop() {
	t.timer.Stop()
}

// ChannelTicker channel定时器
type ChannelTicker struct {
	stop     bool
	delay    int
	interval int
	ch       chan time.Time
}

// NewChannelTicker 创建channel定时器
func NewChannelTicker(interval, delay int) *ChannelTicker {
	return &ChannelTicker{
		delay:    delay,
		interval: interval,
		ch:       make(chan time.Time, 1),
	}
}

// Do channel定时器，delay：延迟执行时间，interval：时间间隔，dofn：定时执行函数
// 此方法时间间隔interval时间
func (ct *ChannelTicker) Do(dofn func(time.Time)) {
	if ct.delay > 0 {
		time.Sleep(time.Duration(ct.delay) * time.Second)
	}

	t := time.Now()
	for {
		if ct.stop {
			break
		}

		go func() {
			ct.ch <- t
			dofn(t)
		}()

		select {
		case st := <-ct.ch:
			now := time.Now()
			t = st.Add(time.Duration(ct.interval) * time.Second)
			d := t.Sub(now)
			time.Sleep(d)
			//time.Sleep(time.Duration(ct.interval) * time.Second)
		}
	}
}

// Stop 停止channel定时器
func (ct *ChannelTicker) Stop() {
	ct.stop = true
}

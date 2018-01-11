// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package persistent

import (
	"sync/atomic"
	"time"

	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/system"
)

type Clock struct {
	precision int64
	now       int64
	closeChan chan bool
}

func NewClock(precision int64) *Clock {
	clock := &Clock{
		precision: precision,
		now:       system.CurrentTimeMillis(),
		closeChan: make(chan bool, 1),
	}
	return clock
}

func (clock *Clock) scheduleClockUpdating(tick *time.Ticker) {
	for {
		select {
		case <-clock.closeChan:
			tick.Stop()
			close(clock.closeChan)
			logger.Info("system clock service close.")
			return
		case <-tick.C:
			atomic.StoreInt64(&clock.now, system.CurrentTimeMillis())
		}
	}
}

func (clock *Clock) Now() int64 {
	return clock.now
}

func (clock *Clock) Start() {
	tick := time.NewTicker(time.Duration(clock.precision) * time.Millisecond)
	clock.scheduleClockUpdating(tick)
}

func (clock *Clock) Shutdown() {
	clock.closeChan <- true
}

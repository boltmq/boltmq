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
package core

import (
	"sync"
	"sync/atomic"
)

type ReferenceResource struct {
	refCount               int64
	available              bool
	cleanupOver            bool
	firstShutdownTimestamp int64
	mutexs                 sync.Mutex
}

func newReferenceResource() *ReferenceResource {
	return &ReferenceResource{
		refCount:               int64(1),
		available:              true,
		cleanupOver:            false,
		firstShutdownTimestamp: 0,
	}
}

func (rf *ReferenceResource) hold() bool {
	rf.mutexs.Lock()
	defer rf.mutexs.Unlock()

	if rf.available {
		atomic.AddInt64(&rf.refCount, 1)
		if atomic.LoadInt64(&rf.refCount) > 0 {
			return true
		} else {
			atomic.AddInt64(&rf.refCount, -1)
		}
	}

	return false
}

func (rf *ReferenceResource) isAvailable() bool {
	return rf.available
}

func (rf *ReferenceResource) isCleanupOver() bool {
	return rf.refCount <= 0 && rf.cleanupOver
}

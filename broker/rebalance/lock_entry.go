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
package rebalance

import (
	"os"
	"strconv"
	"strings"

	"github.com/boltmq/common/utils/system"
	"github.com/boltmq/common/utils/verify"
)

const (
	BROKER_REBLANCE_LOCKMAXLIVETIME = "boltmq.broker.rebalance.lockMaxLiveTime"
)

// LockEntry LockEntry
// Author: rongzhihong
// Since: 2017/9/19
type LockEntry struct {
	ClientId            string
	LastUpdateTimestamp int64
}

func NewLockEntry() *LockEntry {
	entry := new(LockEntry)
	entry.LastUpdateTimestamp = system.CurrentTimeMillis()
	return entry
}

var lockMaxLiveTime int64 = 60000
var lockMaxLiveTimeValue = os.Getenv(BROKER_REBLANCE_LOCKMAXLIVETIME)

func (entry *LockEntry) IsExpired() bool {
	if lockMaxLiveTimeValue != "" && verify.IsNumber(lockMaxLiveTimeValue) {
		lockMaxLiveTime, _ = strconv.ParseInt(lockMaxLiveTimeValue, 10, 64)
	}
	expired := (system.CurrentTimeMillis() - entry.LastUpdateTimestamp) > lockMaxLiveTime
	return expired
}

func (entry *LockEntry) IsLocked(clientId string) bool {
	eq := strings.EqualFold(entry.ClientId, clientId)
	return eq && !entry.IsExpired()
}

package body

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"os"
	"strconv"
	"strings"
)

// LockEntry LockEntry
// Author: rongzhihong, <rongzhihong@gome.com.cn>
// Since: 2017/9/19
type LockEntry struct {
	ClientId            string
	LastUpdateTimestamp int64
}

func NewLockEntry() *LockEntry {
	entry := new(LockEntry)
	entry.LastUpdateTimestamp = timeutil.CurrentTimeMillis()
	return entry
}

var lockMaxLiveTime int64 = 60000
var lockMaxLiveTimeValue = os.Getenv(stgcommon.BROKER_REBLANCE_LOCKMAXLIVETIME)

func (entry *LockEntry) IsExpired() bool {
	if lockMaxLiveTimeValue != "" && stgcommon.IsNumber(lockMaxLiveTimeValue) {
		lockMaxLiveTime, _ = strconv.ParseInt(lockMaxLiveTimeValue, 10, 64)
	}
	expired := (timeutil.CurrentTimeMillis() - entry.LastUpdateTimestamp) > lockMaxLiveTime
	return expired
}

func (entry *LockEntry) IsLocked(clientId string) bool {
	eq := strings.EqualFold(entry.ClientId, clientId)
	return eq && !entry.IsExpired()
}

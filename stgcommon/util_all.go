package stgcommon

import (
	"time"
)

// ComputNextMorningTimeMillis 明天零点时间戳
// Author rongzhihong
// Since 2017/9/5
func ComputNextMorningTimeMillis() int64 {
	currentTime := time.Now()

	nextMorning := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day()+1,
		0, 0, 0, 0, currentTime.Location())

	nextMorningTimeMillis := nextMorning.UnixNano() / 1000000

	return nextMorningTimeMillis
}

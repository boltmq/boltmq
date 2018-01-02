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
package stats

import "time"

// computNextMorningTimeMillis 下一整点天（时、分、秒、毫秒置为0）
// Author rongzhihong
// Since 2017/9/5
func computNextMorningTimeMillis() int64 {
	currentTime := time.Now()
	nextMorning := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day()+1, 0, 0, 0, 0, currentTime.Location())
	nextMorningTimeMillis := nextMorning.UnixNano() / 1000000
	return nextMorningTimeMillis
}

// computNextMinutesTimeMillis 下一整点分钟（秒、毫秒置为0）
// Author rongzhihong
// Since 2017/9/5
func computNextMinutesTimeMillis() int64 {
	currentTime := time.Now()
	nextMorning := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(),
		currentTime.Hour(), currentTime.Minute()+1, 0, 0, currentTime.Location())

	nextMorningTimeMillis := nextMorning.UnixNano() / 1000000
	return nextMorningTimeMillis
}

// computNextHourTimeMillis 下一整点小时（分、秒、毫秒置为0）
// Author rongzhihong
// Since 2017/9/5
func computNextHourTimeMillis() int64 {
	currentTime := time.Now()
	nextMorning := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(),
		currentTime.Hour()+1, 0, 0, 0, currentTime.Location())

	nextMorningTimeMillis := nextMorning.UnixNano() / int64(time.Millisecond)
	return nextMorningTimeMillis
}

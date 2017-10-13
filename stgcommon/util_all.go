package stgcommon

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/statfs"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"github.com/pquerna/ffjson/ffjson"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"
	"strconv"
)

const (
	WINDOWS    = "windows" // windows operating system
	MAX_VALUE  = 0x7fffffffffffffff
	TIMEFORMAT = "2006-01-02 15:04:05"
)

// ComputNextMorningTimeMillis 下一天（时、分、秒、毫秒置为0）
// Author rongzhihong
// Since 2017/9/5
func ComputNextMorningTimeMillis() int64 {
	currentTime := time.Now()
	nextMorning := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day()+1, 0, 0, 0, 0, currentTime.Location())
	nextMorningTimeMillis := nextMorning.UnixNano() / 1000000
	return nextMorningTimeMillis
}

// ComputNextMinutesTimeMillis 下一个分钟（秒、毫秒置为0）
// Author rongzhihong
// Since 2017/9/5
func ComputNextMinutesTimeMillis() int64 {
	currentTime := time.Now()

	nextMorning := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(),
		currentTime.Hour(), currentTime.Minute()+1, 0, 0, currentTime.Location())

	nextMorningTimeMillis := nextMorning.UnixNano() / 1000000

	return nextMorningTimeMillis
}

// ComputNextMinutesTimeMillis 下一小时（分、秒、毫秒置为0）
// Author rongzhihong
// Since 2017/9/5
func ComputNextHourTimeMillis() int64 {
	currentTime := time.Now()

	nextMorning := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(),
		currentTime.Hour()+1, 0, 0, 0, currentTime.Location())

	nextMorningTimeMillis := nextMorning.UnixNano() / int64(time.Millisecond)

	return nextMorningTimeMillis
}

// GetDiskPartitionSpaceUsedPercent 获取磁盘分区空间使用率
// Author rongzhihong
// Since 2017/9/5
func GetDiskPartitionSpaceUsedPercent(path string) (percent float64) {
	defer utils.RecoveredFn()

	percent = -1
	if path != "" {
		isExits, err := ExistsDir(path)
		if err != nil {
			panic(fmt.Sprintf("GetDiskPartitionSpaceUsedPercent error:%s", err.Error()))
			return
		}

		if !isExits {
			os.MkdirAll(path, os.ModePerm)
		}

		diskStatus := statfs.DiskStatfs(path)
		if diskStatus.All > 0 {
			percent = float64(diskStatus.Used) / float64(diskStatus.All)
		}
	}

	return
}

// isWindowsOS check current os is windows
// if current is windows operating system, return true ; otherwise return false
// Author rongzhihong
// Since 2017/9/8
func IsWindowsOS() bool {
	return strings.EqualFold(runtime.GOOS, WINDOWS)
}

// MillsTime2String 将毫秒时间转为字符时间
// Author: rongzhihong, <rongzhihong@gome.com.cn>
// Since: 2017/9/19
func MilliTime2String(millisecond int64) string {
	secondTime := millisecond / 1000
	return time.Unix(secondTime, 0).Format(TIMEFORMAT)
}

var blankReg = regexp.MustCompile(`\S+?`)

var numberReg = regexp.MustCompile(`^[0-9]+?$`)

// IsBlank 是否为空:false:不为空, true:为空
// Author: rongzhihong, <rongzhihong@gome.com.cn>
// Since: 2017/9/19
func IsBlank(content string) bool {
	if blankReg.FindString(content) != "" {
		return false
	}
	return true
}

// IsNumber 是否是数字:true:是, false:否
// Author: rongzhihong, <rongzhihong@gome.com.cn>
// Since: 2017/9/19
func IsNumber(content string) bool {
	return numberReg.MatchString(content)
}

// Encode Json Encode
// Author: rongzhihong, <rongzhihong@gome.com.cn>
// Since: 2017/9/19
func Encode(v interface{}) []byte {
	value, err := ffjson.Marshal(v)
	if err != nil {
		logger.Errorf("json.Encode err: %s", err.Error())
		return nil
	}
	return value
}

// Decode Json Decode
// Author: rongzhihong, <rongzhihong@gome.com.cn>
// Since: 2017/9/19
func Decode(data []byte, v interface{}) error {
	return ffjson.Unmarshal(data, v)
}

// IsItTimeToDo
// Author: zhoufei, <zhoufei17@gome.com.cn>
// Since: 2017/10/13
func IsItTimeToDo(when string) bool {
	whiles := strings.Split(when, ";")
	if whiles != nil && len(whiles) > 0 {
		currentTime := time.Now()

		for i := 0; i < len(whiles); i++ {
			hour, err := strconv.Atoi(whiles[i])
			if err != nil {
				logger.Warn("is it time to do parse time hour, error:", err.Error())
				continue
			}

			if hour == currentTime.Hour() {
				return true
			}
		}
	}

	return false
}

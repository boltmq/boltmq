package stgcommon

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"
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

	nextMorning := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day()+1,
		0, 0, 0, 0, currentTime.Location())

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

// GetDiskPartitionSpaceUsedPercent 此文件已使用磁盘容量占所申请的磁盘容量的百分比
// Author rongzhihong
// Since 2017/9/5
func GetDiskPartitionSpaceUsedPercent(path string) (percent float64) {
	defer utils.RecoveredFn()

	percent = -1.0
	// TODO: unix环境，去掉注释
	/*if path != "" {
		isExits, err := isExists(path)
		if err != nil {
			panic(fmt.Sprintf("GetDiskPartitionSpaceUsedPercent error:%s", err.Error()))
		}

		if !isExits {
			os.MkdirAll(path, os.ModePerm)
		}

		fs := syscall.Statfs_t{}
		err = syscall.Statfs(path, &fs)
		if err != nil {
			return
		}
		totalSpace := fs.Blocks * uint64(fs.Bsize)
		freeSpace := fs.Bfree * uint64(fs.Bsize)
		usedSpace := totalSpace - freeSpace
		if totalSpace > 0 {
			percent = usedSpace / totalSpace
		}
	}*/
	return
}

// FileOrDirIsExists 校验文件或文件夹是否存在
// Author: rongzhihong, <rongzhihong@gome.com.cn>
// Since: 2017/9/13
func isExists(fileFullPath string) (bool, error) {
	_, err := os.Stat(fileFullPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
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

// IsBlank 是否为空:false:不为空, true:为空
// Author: rongzhihong, <rongzhihong@gome.com.cn>
// Since: 2017/9/19
func IsBlank(content string) bool {
	if blankReg.FindString(content) != "" {
		return false
	}
	return true
}

var numberReg = regexp.MustCompile(`^[0-9]+?$`)

// IsNumber 是否是数字:true:是, false:否
// Author: rongzhihong, <rongzhihong@gome.com.cn>
// Since: 2017/9/19
func IsNumber(content string) bool {
	return numberReg.MatchString(content)
}

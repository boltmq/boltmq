package stgcommon

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"os"
	"runtime"
	"strings"
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

const (
	WINDOWS = "windows" // windows operating system
)

// isWindowsOS check current os is windows
// if current is windows operating system, return true ; otherwise return false
// Author rongzhihong
// Since 2017/9/8
func IsWindowsOS() bool {
	return strings.EqualFold(runtime.GOOS, WINDOWS)
}

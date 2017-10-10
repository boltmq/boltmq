package statfs

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"syscall"
	"unsafe"
)

// get Windows disk usage
func diskUsage(path string) (disk DiskStatus) {
	kernel32, err := syscall.LoadLibrary("Kernel32.dll")
	if err != nil {
		logger.Errorf("Load Kernel32 Library throw error:%s\n", err.Error())
		return
	}
	defer syscall.FreeLibrary(kernel32)

	GetDiskFreeSpaceEx, err := syscall.GetProcAddress(syscall.Handle(kernel32), "GetDiskFreeSpaceExW")
	if err != nil {
		logger.Errorf("Get GetDiskFreeSpaceExW Process Address throw error:%s\n", err.Error())
		return
	}

	lpFreeBytesAvailable := int64(0)
	lpTotalNumberOfBytes := int64(0)
	lpTotalNumberOfFreeBytes := int64(0)
	_, _, b := syscall.Syscall6(uintptr(GetDiskFreeSpaceEx), 4,
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&lpFreeBytesAvailable)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfBytes)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfFreeBytes)), 0, 0)

	// error code info: https://baike.baidu.com/item/errno/11040395?fr=aladdin
	if b > 0 {
		logger.Errorf("Get disk usage throw error: errno=%d\n", b)
		return
	}

	disk.All = uint64(lpTotalNumberOfBytes)
	disk.Free = uint64(lpTotalNumberOfFreeBytes)
	disk.Used = disk.All - disk.Free
	return
}

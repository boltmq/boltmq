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
package statfs

import (
	"syscall"
	"unsafe"

	"github.com/go-errors/errors"
)

// get Windows disk usage
func diskUsage(path string) (disk DiskStatus, err error) {
	kernel32, err := syscall.LoadLibrary("Kernel32.dll")
	if err != nil {
		return
	}
	defer syscall.FreeLibrary(kernel32)

	getDiskFreeSpaceEx, err := syscall.GetProcAddress(syscall.Handle(kernel32), "GetDiskFreeSpaceExW")
	if err != nil {
		return
	}

	lpFreeBytesAvailable := int64(0)
	lpTotalNumberOfBytes := int64(0)
	lpTotalNumberOfFreeBytes := int64(0)
	_, _, b := syscall.Syscall6(uintptr(getDiskFreeSpaceEx), 4,
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&lpFreeBytesAvailable)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfBytes)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfFreeBytes)), 0, 0)

	// error code info: https://baike.baidu.com/item/errno/11040395?fr=aladdin
	if b > 0 {
		err = errors.Errorf("disk usage error: errno=%d", b)
		return
	}

	disk.All = uint64(lpTotalNumberOfBytes)
	disk.Free = uint64(lpTotalNumberOfFreeBytes)
	disk.Used = disk.All - disk.Free
	return
}

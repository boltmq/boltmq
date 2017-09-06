// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
package unix

import (
	"reflect"
	"syscall"
	"unsafe"
)

const _SYS_MSYNC = syscall.SYS_MSYNC
const _MS_SYNC = syscall.MS_SYNC

const (
	// RDONLY maps the memory read-only.
	// Attempts to write to the MemoryMap object will result in undefined behavior.
	RDONLY = 0
	// RDWR maps the memory as read-write. Writes to the MemoryMap object will update the
	// underlying file.
	RDWR = 1 << iota
	// COPY maps the memory as copy-on-write. Writes to the MemoryMap object will affect
	// memory, but the underlying file will remain unchanged.
	COPY
	// If EXEC is set, the mapped memory is marked as executable.
	EXEC
)

const (
	ANON = 1 << iota // If the ANON flag is set, the mapped memory will not be backed by a file.
)

// MemoryMap represents a file mapped into memory.
type MemoryMap []byte

func Mmap(len int, inprot, inflags, fd uintptr, off int64) ([]byte, error) {
	flags := syscall.MAP_SHARED
	prot := syscall.PROT_READ
	switch {
	case inprot&COPY != 0:
		prot |= syscall.PROT_WRITE
		flags = syscall.MAP_PRIVATE
	case inprot&RDWR != 0:
		prot |= syscall.PROT_WRITE
	}
	if inprot&EXEC != 0 {
		prot |= syscall.PROT_EXEC
	}
	if inflags&ANON != 0 {
		flags |= syscall.MAP_ANON
	}

	b, err := syscall.Mmap(int(fd), off, len, prot, flags)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func Flush(addr, len uintptr) error {
	_, _, errno := syscall.Syscall(_SYS_MSYNC, addr, len, _MS_SYNC)
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return nil
}

func Lock(addr, len uintptr) error {
	_, _, errno := syscall.Syscall(syscall.SYS_MLOCK, addr, len, 0)
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return nil
}

func Unlock(addr, len uintptr) error {
	_, _, errno := syscall.Syscall(syscall.SYS_MUNLOCK, addr, len, 0)
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return nil
}

func Unmap(addr, len uintptr) error {
	_, _, errno := syscall.Syscall(syscall.SYS_MUNMAP, addr, len, 0)
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return nil
}

func (m *MemoryMap) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(m))
}

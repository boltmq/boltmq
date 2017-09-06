// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
package unix

import (
	"reflect"
	"sync"
	"syscall"
	"unsafe"
)

// 如下常量定义只是为了能在windows编译通过，严格的值应该从"syscall.SYS_MSYNC"之类的属性读取
// 此处为了编译通过，从 centos7.0操作系统 amd64架构 golang1.8版本获取
// 此类各类常量位于: go源码 src/syscall/*_linux_amd64.go文件定义
// 常量定义文档：https://golang.org/pkg/syscall/
const SYS_MSYNC = 26    // syscall.SYS_MSYNC, 常量位于 go1.8/src/syscall/zsysnum_linux_amd64.go
const MS_SYNC = 0x4     // syscall.MS_SYNC, 常量位于 go1.8/src/syscall/zerrors_linux_amd64.go
const MAP_SHARED = 0x1  // syscall.MAP_SHARED, 常量位于 go1.8/src/syscall/zerrors_linux_amd64.go
const PROT_READ = 0x1   // syscall.PROT_READ, 常量位于 go1.8/src/syscall/zerrors_linux_amd64.go
const PROT_WRITE = 0x2  // syscall.PROT_WRITE, 常量位于 go1.8/src/syscall/zerrors_linux_amd64.go
const MAP_PRIVATE = 0x2 // syscall.MAP_PRIVATE, 常量位于 go1.8/src/syscall/zerrors_linux_amd64.go
const MAP_ANON = 0x20   // syscall.MAP_ANON, 常量位于 go1.8/src/syscall/zerrors_linux_amd64.go
const PROT_EXEC = 0x4   // syscall.PROT_EXEC, 常量位于 go1.8/src/syscall/zerrors_linux_amd64.go
const SYS_MUNMAP = 11   // syscall.SYS_MUNMAP, 常量位于 go1.8/src/syscall/zsysnum_linux_amd64.go
const SYS_MUNLOCK = 150 // syscall.SYS_MUNLOCK, 常量位于 go1.8/src/syscall/zsysnum_linux_amd64.go
const SYS_MLOCK = 149   // syscall.SYS_MLOCK, 常量位于 go1.8/src/syscall/zsysnum_linux_amd64.go
const EINVAL = 0x16     // Errno(0x16), 常量位于 go1.8/src/syscall/zerrors_linux_amd64.go

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
	flags := MAP_SHARED
	prot := PROT_READ
	switch {
	case inprot&COPY != 0:
		prot |= PROT_WRITE
		flags = MAP_PRIVATE
	case inprot&RDWR != 0:
		prot |= PROT_WRITE
	}
	if inprot&EXEC != 0 {
		prot |= PROT_EXEC
	}
	if inflags&ANON != 0 {
		flags |= MAP_ANON
	}

	// b, err := syscall.Mmap(int(fd), off, len, prot, flags)
	// 原本是调用“syscall.Mmap()”,因为在windows系统编译不通过，故修改之 Modify: tianyuliang Since: 2017/9/6
	b, err := syscallMmap(int(fd), off, len, prot, flags)
	if err != nil {
		return nil, err
	}
	return b, nil
}

type mmapper struct {
	sync.Mutex
	active map[*byte][]byte // active mappings; key is last byte in mapping
	mmap   func(addr, length uintptr, prot, flags, fd int, offset int64) (uintptr, error)
	munmap func(addr uintptr, length uintptr) error
}

func syscallMmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error) {
	if length <= 0 {
		return nil, syscall.Errno(EINVAL)
	}

	// Map the requested memory.
	addr, errno := mmap(0, uintptr(length), prot, flags, fd, offset)
	if errno != nil {
		return nil, errno
	}

	// Slice memory layout
	var sl = struct {
		addr uintptr
		len  int
		cap  int
	}{
		addr,
		length,
		length,
	}

	// Use unsafe to turn sl into a []byte.
	b := *(*[]byte)(unsafe.Pointer(&sl))

	// Register mapping in m and return it.
	p := &b[cap(b)-1]

	m := mmapper{
		Mutex:  sync.Mutex{},
		active: make(map[*byte][]byte),
	}

	m.Lock()
	defer m.Unlock()
	m.active[p] = b
	return b, nil
}

func mmap(addr, length uintptr, prot, flags, fd int, offset int64) (uintptr, error) {
	//TODO: 此处的mmap()是内核系统方法，但windows编译不通过,此处只是为了编译，并不表示能够正常运行
	return 0, nil
}

func Flush(addr, len uintptr) error {
	_, _, errno := syscall.Syscall(SYS_MSYNC, addr, len, MS_SYNC, 0)
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return nil
}

func Lock(addr, len uintptr) error {
	_, _, errno := syscall.Syscall(SYS_MLOCK, addr, len, 0, 0)
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return nil
}

func Unlock(addr, len uintptr) error {
	_, _, errno := syscall.Syscall(SYS_MUNLOCK, addr, len, 0, 0)
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return nil
}

func Unmap(addr, len uintptr) error {
	_, _, errno := syscall.Syscall(SYS_MUNMAP, addr, len, 0, 0)
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return nil
}

func (m *MemoryMap) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(m))
}

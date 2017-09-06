// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
package windows

import (
	"errors"
	"os"
	"reflect"
	"sync"
	"syscall"
	"unsafe"
)

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

// mmap on Windows is a two-step process.
// First, we call CreateFileMapping to get a handle.
// Then, we call MapviewToFile to get an actual pointer into memory.
// Because we want to emulate a POSIX-style mmap, we don't want to expose
// the handle -- only the pointer. We also want to return only a byte slice,
// not a struct, so it's convenient to manipulate.

// We keep this map so that we can get back the original handle from the memory address.
var handleLock sync.Mutex
var handleMap = map[uintptr]syscall.Handle{}

func Mmap(len int, prot, flags, hfile uintptr, off int64) ([]byte, error) {
	flProtect := uint32(syscall.PAGE_READONLY)
	dwDesiredAccess := uint32(syscall.FILE_MAP_READ)
	switch {
	case prot&COPY != 0:
		flProtect = syscall.PAGE_WRITECOPY
		dwDesiredAccess = syscall.FILE_MAP_COPY
	case prot&RDWR != 0:
		flProtect = syscall.PAGE_READWRITE
		dwDesiredAccess = syscall.FILE_MAP_WRITE
	}
	if prot&EXEC != 0 {
		flProtect <<= 4
		dwDesiredAccess |= syscall.FILE_MAP_EXECUTE
	}

	// The maximum size is the area of the file, starting from 0,
	// that we wish to allow to be mappable. It is the sum of
	// the length the user requested, plus the offset where that length
	// is starting from. This does not map the data into memory.
	maxSizeHigh := uint32((off + int64(len)) >> 32)
	maxSizeLow := uint32((off + int64(len)) & 0xFFFFFFFF)
	// TODO: Do we need to set some security attributes? It might help portability.
	h, errno := syscall.CreateFileMapping(syscall.Handle(hfile), nil, flProtect, maxSizeHigh, maxSizeLow, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	// Actually map a view of the data into memory. The view's size
	// is the length the user requested.
	fileOffsetHigh := uint32(off >> 32)
	fileOffsetLow := uint32(off & 0xFFFFFFFF)
	addr, errno := syscall.MapViewOfFile(h, dwDesiredAccess, fileOffsetHigh, fileOffsetLow, uintptr(len))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}
	handleLock.Lock()
	handleMap[addr] = h
	handleLock.Unlock()

	m := MemoryMap{}
	dh := m.header()
	dh.Data = addr
	dh.Len = len
	dh.Cap = dh.Len

	return m, nil
}

func Flush(addr, len uintptr) error {
	errno := syscall.FlushViewOfFile(addr, len)
	if errno != nil {
		return os.NewSyscallError("FlushViewOfFile", errno)
	}

	handleLock.Lock()
	defer handleLock.Unlock()
	handle, ok := handleMap[addr]
	if !ok {
		// should be impossible; we would've errored above
		return errors.New("unknown base address")
	}

	errno = syscall.FlushFileBuffers(handle)
	return os.NewSyscallError("FlushFileBuffers", errno)
}

func Lock(addr, len uintptr) error {
	errno := syscall.VirtualLock(addr, len)
	return os.NewSyscallError("VirtualLock", errno)
}

func Unlock(addr, len uintptr) error {
	errno := syscall.VirtualUnlock(addr, len)
	return os.NewSyscallError("VirtualUnlock", errno)
}

func Unmap(addr, len uintptr) error {
	Flush(addr, len)
	// Lock the UnmapViewOfFile along with the handleMap deletion.
	// As soon as we unmap the view, the OS is free to give the
	// same addr to another new map. We don't want another goroutine
	// to insert and remove the same addr into handleMap while
	// we're trying to remove our old addr/handle pair.
	handleLock.Lock()
	defer handleLock.Unlock()
	err := syscall.UnmapViewOfFile(addr)
	if err != nil {
		return err
	}

	handle, ok := handleMap[addr]
	if !ok {
		// should be impossible; we would've errored above
		return errors.New("unknown base address")
	}
	delete(handleMap, addr)

	e := syscall.CloseHandle(syscall.Handle(handle))
	return os.NewSyscallError("CloseHandle", e)
}

func (m *MemoryMap) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(m))
}

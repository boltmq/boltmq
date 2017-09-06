// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
package mmap

import (
	"errors"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/mmap/unix"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/mmap/windows"
	"os"
	"reflect"
	"runtime"
	"strings"
	"unsafe"
)

const (
	ANON = 1 << iota // If the ANON flag is set, the mapped memory will not be backed by a file.
)

const (
	WINDOWS = "windows" // windows operating system
)

// MemoryMap represents a file mapped into memory.
type MemoryMap []byte

// Map maps an entire file into memory.
// If ANON is set in flags, f is ignored.
func Map(f *os.File, prot, flags int) (MemoryMap, error) {
	return MapRegion(f, -1, prot, flags, 0)
}

// MapRegion maps part of a file into memory.
// The offset parameter must be a multiple of the system's page size.
// If length < 0, the entire file will be mapped.
// If ANON is set in flags, f is ignored.
func MapRegion(f *os.File, length int, prot, flags int, offset int64) (MemoryMap, error) {
	if offset%int64(os.Getpagesize()) != 0 {
		return nil, errors.New("offset parameter must be a multiple of the system's page size")
	}

	var fd uintptr
	if flags&ANON == 0 {
		fd = uintptr(f.Fd())
		if length < 0 {
			fi, err := f.Stat()
			if err != nil {
				return nil, err
			}
			length = int(fi.Size())
		}
	} else {
		if length <= 0 {
			return nil, errors.New("anonymous mapping requires non-zero length")
		}
		fd = ^uintptr(0)
	}

	if isWindowsOS() {
		return windows.Mmap(length, uintptr(prot), uintptr(flags), fd, offset)
	}

	return unix.Mmap(length, uintptr(prot), uintptr(flags), fd, offset)
}

func (m *MemoryMap) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(m))
}

// Lock keeps the mapped region in physical memory, ensuring that it will not be
// swapped out.
func (m MemoryMap) Lock() error {
	dh := m.header()

	if isWindowsOS() {
		return windows.Lock(dh.Data, uintptr(dh.Len))
	}

	return unix.Lock(dh.Data, uintptr(dh.Len))
}

// Unlock reverses the effect of Lock, allowing the mapped region to potentially
// be swapped out.
// If m is already unlocked, aan error will result.
func (m MemoryMap) Unlock() error {
	dh := m.header()

	if isWindowsOS() {
		return windows.Unlock(dh.Data, uintptr(dh.Len))
	}

	return unix.Unlock(dh.Data, uintptr(dh.Len))
}

// flush synchronizes the mapping's contents to the file's contents on disk.
func (m MemoryMap) Flush() error {
	dh := m.header()

	if isWindowsOS() {
		return windows.Flush(dh.Data, uintptr(dh.Len))
	}

	return unix.Flush(dh.Data, uintptr(dh.Len))
}

// unmap deletes the memory mapped region, flushes any remaining changes, and sets
// m to nil.
// Trying to read or write any remaining references to m after unmap is called will
// result in undefined behavior.
// unmap should only be called on the slice value that was originally returned from
// a call to Map. Calling unmap on a derived slice may cause errors.
func (m *MemoryMap) Unmap() (err error) {
	dh := m.header()
	if isWindowsOS() {
		err = windows.Unmap(dh.Data, uintptr(dh.Len))
	} else {
		err = unix.Unmap(dh.Data, uintptr(dh.Len))
	}

	*m = nil
	return err
}

// isWindowsOS check current os is windows
// if current is windows operating system, return true ; otherwise return false
func isWindowsOS() bool {
	return strings.EqualFold(runtime.GOOS, WINDOWS)
}

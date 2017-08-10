// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
package mmap

import (
	"errors"
	"os"
	"reflect"
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
	// If the ANON flag is set, the mapped memory will not be backed by a file.
	ANON = 1 << iota
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
	return mmap(length, uintptr(prot), uintptr(flags), fd, offset)
}

func (m *MemoryMap) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(m))
}

// Lock keeps the mapped region in physical memory, ensuring that it will not be
// swapped out.
func (m MemoryMap) Lock() error {
	dh := m.header()
	return lock(dh.Data, uintptr(dh.Len))
}

// Unlock reverses the effect of Lock, allowing the mapped region to potentially
// be swapped out.
// If m is already unlocked, aan error will result.
func (m MemoryMap) Unlock() error {
	dh := m.header()
	return unlock(dh.Data, uintptr(dh.Len))
}

// flush synchronizes the mapping's contents to the file's contents on disk.
func (m MemoryMap) Flush() error {
	dh := m.header()

	return flush(dh.Data, uintptr(dh.Len))
}

// unmap deletes the memory mapped region, flushes any remaining changes, and sets
// m to nil.
// Trying to read or write any remaining references to m after unmap is called will
// result in undefined behavior.
// unmap should only be called on the slice value that was originally returned from
// a call to Map. Calling unmap on a derived slice may cause errors.
func (m *MemoryMap) Unmap() error {
	dh := m.header()
	err := unmap(dh.Data, uintptr(dh.Len))
	*m = nil
	return err
}

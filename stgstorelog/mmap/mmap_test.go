// Copyright (c) 2015-2018 All rights reserved.
// 本软件源代码版权归 my.oschina.net/tantexian 所有,允许复制与学习借鉴.
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/8/5
package mmap

import (
	"bytes"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
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

var testData = []byte("0123456789ABCDEF")

//var testPath = filepath.Join(os.TempDir(), "testdata")
var testPath = filepath.Join("./", "testdata.tmp")

func init() {
	f := openFile(os.O_RDWR | os.O_CREATE | os.O_TRUNC)
	f.Write(testData)
	f.Close()
}

func openFile(flags int) *os.File {
	f, err := os.OpenFile(testPath, flags, 0644)
	if err != nil {
		panic(err.Error())
	}
	return f
}

func TestUnmap(t *testing.T) {
	f := openFile(os.O_RDONLY)
	defer f.Close()
	mmap, err := Map(f, RDONLY, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	if err := mmap.Unmap(); err != nil {
		t.Errorf("error unmapping: %s", err)
	}
}

func TestReadWrite(t *testing.T) {
	f := openFile(os.O_RDWR)
	defer f.Close()
	mmap, err := Map(f, RDWR, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	defer mmap.Unmap()
	if !bytes.Equal(testData, mmap) {
		t.Errorf("mmap != testData: %q, %q", mmap, testData)
	}

	mmap[9] = 'X'
	mmap.Flush()

	logger.Info("mmap == %v", string(mmap))

	fileData, err := ioutil.ReadAll(f)
	if err != nil {
		t.Errorf("error reading file: %s", err)
	}
	if !bytes.Equal(fileData, []byte("012345678XABCDEF")) {
		t.Errorf("file wasn't modified")
	}

	// leave things how we found them
	mmap[9] = '9'
	mmap.Flush()
	logger.Info("mmap == %v", string(mmap))
}

func TestProtFlagsAndErr(t *testing.T) {
	f := openFile(os.O_RDONLY)
	defer f.Close()
	if _, err := Map(f, RDWR, 0); err == nil {
		t.Errorf("expected error")
	}
}

func TestFlags(t *testing.T) {
	f := openFile(os.O_RDWR)
	defer f.Close()
	mmap, err := Map(f, COPY, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	defer mmap.Unmap()

	mmap[9] = 'X'
	mmap.Flush()

	fileData, err := ioutil.ReadAll(f)
	if err != nil {
		t.Errorf("error reading file: %s", err)
	}
	if !bytes.Equal(fileData, testData) {
		t.Errorf("file was modified")
	}
}

// Test that we can map files from non-0 offsets
// The page size on most Unixes is 4KB, but on Windows it's 64KB
func TestNonZeroOffset(t *testing.T) {
	const pageSize = 65536

	// Create a 2-page sized file
	bigFilePath := filepath.Join( /*os.TempDir()*/ "./", "nonzero.tmp")
	fileobj, err := os.OpenFile(bigFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err.Error())
	}

	bigData := make([]byte, 2*pageSize, 2*pageSize)
	fileobj.Write(bigData)
	fileobj.Close()

	// Map the first page by itself
	fileobj, err = os.OpenFile(bigFilePath, os.O_RDONLY, 0)
	if err != nil {
		panic(err.Error())
	}
	m, err := MapRegion(fileobj, pageSize, RDONLY, 0, 0)
	if err != nil {
		t.Errorf("error mapping file: %s", err)
	}
	m.Unmap()
	fileobj.Close()

	// Map the second page by itself
	fileobj, err = os.OpenFile(bigFilePath, os.O_RDONLY, 0)
	if err != nil {
		panic(err.Error())
	}
	m, err = MapRegion(fileobj, pageSize, RDONLY, 0, pageSize)
	if err != nil {
		t.Errorf("error mapping file: %s", err)
	}
	err = m.Unmap()
	if err != nil {
		t.Error(err)
	}

	m, err = MapRegion(fileobj, pageSize, RDONLY, 0, 1)
	if err == nil {
		t.Error("expect error because offset is not multiple of page size")
	}

	fileobj.Close()
}

func TestRandReadWrite(t *testing.T) {
	f, err := os.OpenFile("./testRandRW.tmp", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err.Error())
	}
	i := 0
	for i <= 1000 {
		f.WriteString(strconv.Itoa(i))
		f.WriteString(" ")
		if i%10 == 0 {
			f.WriteString("\n")
		}
		i++
	}

	defer f.Close()
	mmap, err := Map(f, RDWR, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	defer mmap.Unmap()

	mmap[11] = 'X'
	mmap.Flush()

	//logger.Info("mmap == %v", string(mmap))
}

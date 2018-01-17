// Copyright 2017 tantexian

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// These tests are adapted from gommap: http://labix.org/gommap
// Copyright (c) 2010, Gustavo Niemeyer <gustavo@niemeyer.net>

package mmap

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

var testData = []byte("0123456789ABCDEF")
var testPath = filepath.Join(os.TempDir(), "testdata")

func prepare() {
	f := openFile(os.O_RDWR | os.O_CREATE | os.O_TRUNC)
	f.Write(testData)
	f.Close()
}

func recover() {
	if err := os.Remove(testPath); err != nil {
		panic(err)
	}
}

func openFile(flags int) *os.File {
	f, err := os.OpenFile(testPath, flags, 0644)
	if err != nil {
		panic(err.Error())
	}
	return f
}

func TestUnmap(t *testing.T) {
	prepare()

	f := openFile(os.O_RDONLY)
	defer f.Close()
	mmap, err := Map(f, RDONLY, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	if err := mmap.Unmap(); err != nil {
		t.Errorf("error unmapping: %s", err)
	}

	recover()
}

func TestReadWrite(t *testing.T) {
	prepare()

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

	recover()
}

func TestProtFlagsAndErr(t *testing.T) {
	prepare()

	f := openFile(os.O_RDONLY)
	defer f.Close()
	if _, err := Map(f, RDWR, 0); err == nil {
		t.Errorf("expected error")
	}

	recover()
}

func TestFlags(t *testing.T) {
	prepare()

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

	recover()
}

// Test that we can map files from non-0 offsets
// The page size on most Unixes is 4KB, but on Windows it's 64KB
func TestNonZeroOffset(t *testing.T) {
	prepare()

	const pageSize = 65536

	// Create a 2-page sized file
	testPath := filepath.Join(os.TempDir(), "nonzero")
	fileobj, err := os.OpenFile(testPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err.Error())
	}

	bigData := make([]byte, 2*pageSize, 2*pageSize)
	fileobj.Write(bigData)
	fileobj.Close()

	// Map the first page by itself
	fileobj, err = os.OpenFile(testPath, os.O_RDONLY, 0)
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
	fileobj, err = os.OpenFile(testPath, os.O_RDONLY, 0)
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

	recover()
}

func TestRandReadWrite(t *testing.T) {
	prepare()

	f := openFile(os.O_RDWR)
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

	//log.Printf("mmap == %v\n", string(mmap))
	recover()
}

func BenchmarkReadWrite(b *testing.B) {
	var (
		fileSize   int64 = 1024 * 1024 * 1024
		f          *os.File
		e          error
		wSizeEvery int = 200
	)

	b.StopTimer()
	f, e = os.OpenFile(testPath, os.O_RDWR|os.O_CREATE, 0644)
	if e != nil {
		b.Error(e)
		return
	}

	if e = os.Truncate(testPath, fileSize); e != nil {
		b.Error(e)
		return
	}

	b.StartTimer()
	//b.ResetTimer()
	m, e := Map(f, RDWR, 0)
	if e != nil {
		b.Error(e)
		return
	}

	bytes := newBytes(wSizeEvery)

	for i := 0; i < b.N; i++ {
		//if i >= int(fileSize) {
		//break
		//}
		m[i] = bytes[i%wSizeEvery]
	}

	e = m.Flush()
	if e != nil {
		b.Error(e)
		return
	}

	e = m.Unmap()
	if e != nil {
		b.Error(e)
		return
	}

	recover()
}

func newBytes(size int) []byte {
	bytes := make([]byte, size)
	for i := 0; i < size; i++ {
		bytes[i] = byte(i + 1)
	}

	return bytes
}

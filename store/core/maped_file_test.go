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
package core

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var testFile = filepath.Join(os.TempDir(), "testdata/001")

func remove() {
	if err := os.RemoveAll(parentDirectory(testFile)); err != nil {
		panic(err)
	}
}

func TestOpenMapedFile(t *testing.T) {
	var (
		size int64 = 1024 * 64
	)

	mapFile, err := NewMapedFile(testFile, size)
	if err != nil {
		t.Errorf("NewMapedFile:%s", err)
		return
	}

	maps := mapFile.mappedByteBuffer.MMapBuf
	if int(size) != len(maps) {
		t.Errorf("NewMapedFile size is %d not equal expect %d", len(maps), size)
	}

	remove()
}

func TestMapedFile_Write(t *testing.T) {
	var (
		size int64 = 1024 * 64
	)

	mapFile, err := NewMapedFile(testFile, size)
	if err != nil {
		t.Errorf("NewMapedFile:%s", err)
		return
	}

	for i := 1; i <= 64; i++ {
		msg := fmt.Sprintf("helloword%d", i)
		mapFile.appendMessage([]byte(msg))
	}
	mapFile.Flush()
	mapFile.Unmap()

	remove()
}

func TestMapedFile_MMapBufferWithInt32(t *testing.T) {
	var (
		size int64 = 1024 * 64
	)

	mapFile, err := NewMapedFile(testFile, size)
	if err != nil {
		t.Errorf("NewMapedFile:%s", err)
		return
	}

	byteBuffer := mapFile.mappedByteBuffer
	for i := 1; i <= 1024; i++ {
		byteBuffer.WriteInt32(int32(i))
	}

	for i := 1; i <= 1024; i++ {
		tv := byteBuffer.ReadInt32()
		if int32(i) != tv {
			t.Errorf("read %d != write %d", tv, i)
			break
		}
	}
	mapFile.Flush()
	mapFile.Unmap()

	remove()
}

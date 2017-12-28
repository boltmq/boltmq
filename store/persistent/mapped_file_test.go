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
package persistent

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

func TestOpenMappedFile(t *testing.T) {
	var (
		size int64 = 1024 * 64
	)

	mapFile, err := newMappedFile(testFile, size)
	if err != nil {
		t.Errorf("newMappedFile:%s", err)
		return
	}

	maps := mapFile.byteBuffer.mmapBuf
	if int(size) != len(maps) {
		t.Errorf("newMappedFile size is %d not equal expect %d", len(maps), size)
	}

	remove()
}

func TestMappedFile_Write(t *testing.T) {
	var (
		size int64 = 1024 * 64
	)

	mapFile, err := newMappedFile(testFile, size)
	if err != nil {
		t.Errorf("newMappedFile:%s", err)
		return
	}

	for i := 1; i <= 64; i++ {
		msg := fmt.Sprintf("helloword%d", i)
		mapFile.appendMessage([]byte(msg))
	}
	mapFile.flush()
	mapFile.unmap()

	remove()
}

func TestMappedFile_MMapBufferWithInt32(t *testing.T) {
	var (
		size int64 = 1024 * 64
	)

	mapFile, err := newMappedFile(testFile, size)
	if err != nil {
		t.Errorf("newMappedFile:%s", err)
		return
	}

	byteBuffer := mapFile.byteBuffer
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
	mapFile.flush()
	mapFile.unmap()

	remove()
}

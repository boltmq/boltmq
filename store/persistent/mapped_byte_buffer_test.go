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
	"testing"
)

func TestNewMappedByteBuffer(t *testing.T) {
	var (
		myBytes = make([]byte, 100)
		ival    int32
	)

	buffer := newMappedByteBuffer(myBytes)
	buffer.WriteInt32(1)
	buffer.WriteInt32(20080808)
	buffer.WriteInt32(9)
	buffer.Write([]byte("Hello"))

	ival = buffer.ReadInt32()
	if ival != 1 {
		t.Errorf("read error")
		return
	}

	ival = buffer.ReadInt32()
	if ival != 20080808 {
		t.Errorf("read error")
		return
	}

	ival = buffer.ReadInt32()
	if ival != 9 {
		t.Errorf("read error")
		return
	}

	data := make([]byte, 5)
	_, e := buffer.Read(data)
	if e != nil {
		t.Errorf("read error")
		return
	}
}

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
package remoting

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/boltmq/boltmq/net/core"
)

func TestPackFull(t *testing.T) {
	var (
		addr                               = core.AddrToSocketAddr("192.168.0.1:10000")
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 0)
	)

	buffer := prepareFullBuffer()

	bufs, e := lengthFieldFragmentationAssemblage.Pack(addr, buffer)
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if len(bufs) != 1 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", len(bufs), 1)
	}

	buf := bufs[0]
	if !reflect.DeepEqual(buf.Bytes(), buffer) {
		t.Errorf("Test failed: return buf%v incorrect, expect%v", buf.Bytes(), buffer)
	}
}

func TestPackOffsetFull(t *testing.T) {
	var (
		addr                               = core.AddrToSocketAddr("192.168.0.1:10000")
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 4)
	)

	buffer := prepareFullBuffer()

	bufs, e := lengthFieldFragmentationAssemblage.Pack(addr, buffer)
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if len(bufs) != 1 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", len(bufs), 1)
	}

	buf := bufs[0]
	if !reflect.DeepEqual(buf.Bytes(), buffer[4:]) {
		t.Errorf("Test failed: return buf%v incorrect, expect%v", buf.Bytes(), buffer[4:])
	}
}

func prepareFullBuffer() []byte {
	var (
		buf          = bytes.NewBuffer([]byte{})
		length int32 = 10
	)
	binary.Write(buf, binary.BigEndian, length)
	buf.Write([]byte("abcdefghij"))

	return buf.Bytes()
}

func TestPackPartOf(t *testing.T) {
	var (
		addr                               = core.AddrToSocketAddr("192.168.0.1:10000")
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 0)
	)

	buffer := preparePartOfOneBuffer()
	bufs, e := lengthFieldFragmentationAssemblage.Pack(addr, buffer)
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if len(bufs) != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", len(bufs), 0)
	}
	bufferHeader := buffer

	buffer = preparePartOfTwoBuffer()
	bufs, e = lengthFieldFragmentationAssemblage.Pack(addr, buffer)
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if len(bufs) != 1 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", len(bufs), 1)
	}

	buf := bufs[0]
	allBytes := append(bufferHeader, buffer...)
	if !reflect.DeepEqual(buf.Bytes(), allBytes) {
		t.Errorf("Test failed: return buf%v incorrect, expect%v", buf.Bytes(), allBytes)
	}
}

func TestPackOffsetPartOf(t *testing.T) {
	var (
		addr                               = core.AddrToSocketAddr("192.168.0.1:10000")
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 4)
	)

	buffer := preparePartOfOneBuffer()
	bufs, e := lengthFieldFragmentationAssemblage.Pack(addr, buffer)
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if len(bufs) != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", len(bufs), 0)
	}
	bufferHeader := buffer[4:]

	buffer = preparePartOfTwoBuffer()
	bufs, e = lengthFieldFragmentationAssemblage.Pack(addr, buffer)
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if len(bufs) != 1 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", len(bufs), 1)
	}

	buf := bufs[0]
	allBytes := append(bufferHeader, buffer...)
	if !reflect.DeepEqual(buf.Bytes(), allBytes) {
		t.Errorf("Test failed: return buf%v incorrect, expect%v", buf.Bytes(), allBytes)
	}
}

func preparePartOfOneBuffer() []byte {
	var (
		buf          = bytes.NewBuffer([]byte{})
		length int32 = 10
	)
	binary.Write(buf, binary.BigEndian, length)

	return buf.Bytes()
}

func preparePartOfTwoBuffer() []byte {
	var (
		buf = bytes.NewBuffer([]byte{})
	)
	buf.Write([]byte("abcdefghij"))

	return buf.Bytes()
}

func TestPackPartOf2(t *testing.T) {
	var (
		addr                               = core.AddrToSocketAddr("192.168.0.1:10000")
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 0)
	)

	buffer := preparePartOfOneBuffer2()
	bufs, e := lengthFieldFragmentationAssemblage.Pack(addr, buffer)
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if len(bufs) != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", len(bufs), 0)
	}
	bufferHeader := buffer

	buffer = preparePartOfTwoBuffer2()
	bufs, e = lengthFieldFragmentationAssemblage.Pack(addr, buffer)
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if len(bufs) != 1 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", len(bufs), 1)
	}

	buf := bufs[0]
	allBytes := append(bufferHeader, buffer...)
	allBytes = allBytes[:buf.Len()]
	if !reflect.DeepEqual(buf.Bytes(), allBytes) {
		t.Errorf("Test failed: return buf%v incorrect, expect%v", buf.Bytes(), allBytes)
	}
}

func TestPackOffsetPartOf2(t *testing.T) {
	var (
		addr                               = core.AddrToSocketAddr("192.168.0.1:10000")
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 4)
	)

	buffer := preparePartOfOneBuffer2()
	bufs, e := lengthFieldFragmentationAssemblage.Pack(addr, buffer)
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if len(bufs) != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", len(bufs), 0)
	}
	bufferHeader := buffer[4:]

	buffer = preparePartOfTwoBuffer2()
	bufs, e = lengthFieldFragmentationAssemblage.Pack(addr, buffer)
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if len(bufs) != 1 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", len(bufs), 1)
	}

	buf := bufs[0]
	allBytes := append(bufferHeader, buffer...)
	allBytes = allBytes[:buf.Len()]
	if !reflect.DeepEqual(buf.Bytes(), allBytes) {
		t.Errorf("Test failed: return buf%v incorrect, expect%v", buf.Bytes(), allBytes)
	}
}

func preparePartOfOneBuffer2() []byte {
	var (
		buf          = bytes.NewBuffer([]byte{})
		length int32 = 10
	)
	binary.Write(buf, binary.BigEndian, length)
	buf.Write([]byte("abcd"))

	return buf.Bytes()
}

func preparePartOfTwoBuffer2() []byte {
	var (
		buf          = bytes.NewBuffer([]byte{})
		length int32 = 10
	)
	buf.Write([]byte("efghij"))
	binary.Write(buf, binary.BigEndian, length)
	buf.Write([]byte("abc"))

	return buf.Bytes()
}

func TestDiscardPackOffsetPartOf(t *testing.T) {
	var (
		addr                               = core.AddrToSocketAddr("192.168.0.1:10000")
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 0)
	)

	buffer := preparePartOfOneDiscardBuffer()
	bufs, e := lengthFieldFragmentationAssemblage.Pack(addr, buffer)
	if e != nil {
		t.Logf("Pack failed: %s", e)
	}

	if len(bufs) != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", len(bufs), 0)
	}

	buffer = preparePartOfTwoDiscardBuffer()
	bufs, e = lengthFieldFragmentationAssemblage.Pack(addr, buffer)
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if len(bufs) != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", len(bufs), 0)
	}
}

func TestDiscardPackPartOf(t *testing.T) {
	var (
		addr                               = core.AddrToSocketAddr("192.168.0.1:10000")
		lengthFieldFragmentationAssemblage = NewLengthFieldFragmentationAssemblage(8388608, 0, 4, 4)
	)

	buffer := preparePartOfOneDiscardBuffer()
	bufs, e := lengthFieldFragmentationAssemblage.Pack(addr, buffer)
	if e != nil {
		t.Logf("Pack failed: %s", e)
	}

	if len(bufs) != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", len(bufs), 0)
	}

	buffer = preparePartOfTwoDiscardBuffer()
	bufs, e = lengthFieldFragmentationAssemblage.Pack(addr, buffer)
	if e != nil {
		t.Errorf("Test failed: %s", e)
	}

	if len(bufs) != 0 {
		t.Errorf("Test failed: return buf size[%d] incorrect, expect[%d]", len(bufs), 0)
	}
}

func preparePartOfOneDiscardBuffer() []byte {
	var (
		buf = bytes.NewBuffer([]byte{})
	)
	buf.Write([]byte("abcdefghij"))

	return buf.Bytes()
}

func preparePartOfTwoDiscardBuffer() []byte {
	var (
		buf          = bytes.NewBuffer([]byte{})
		length int32 = 10
	)
	binary.Write(buf, binary.BigEndian, length)

	return buf.Bytes()
}

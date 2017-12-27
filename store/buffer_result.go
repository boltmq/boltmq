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
package store

type BufferResult interface {
	Release()
	Buffer() ByteBuffer
	Size() int
}

// ByteBuffer
type ByteBuffer interface {
	Bytes() []byte

	Write([]byte) (int, error)
	WriteInt8(int8) error
	WriteInt16(int16) error
	WriteInt32(int32) error
	WriteInt64(int64) error

	Read([]byte) (int, error)
	ReadInt8() int8
	ReadInt16() int16
	ReadInt32() int32
	ReadInt64() int64
}

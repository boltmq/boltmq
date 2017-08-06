// Copyright 2017 The Authors. All rights reserved.
// Use of this source code is governed by a Apache 
// license that can be found in the LICENSE file.
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/6
package stgstorelog

import "bytes"
import (
	"git.oschina.net/cloudzone/smartgo/stgstorelog/mmap"
	"errors"
	"io"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
)

type MappedByteBuffer struct {
	mMapBuf   mmap.MemoryMap
	off       int      // read at &buf[off], write at &buf[len(buf)]
	bootstrap [64]byte // memory to hold first slice; helps small buffers avoid allocation.
	lastRead  readOp   // last read operation, so that Unread* can work correctly.
}

// The readOp constants describe the last action performed on
// the buffer, so that UnreadRune and UnreadByte can check for
// invalid usage. opReadRuneX constants are chosen such that
// converted to int they correspond to the rune size that was read.
type readOp int

const (
	opRead      readOp = -1 // Any other read operation.
	opInvalid          = 0  // Non-read operation.
	opReadRune1        = 1  // Read rune of size 1.
	opReadRune2        = 2  // Read rune of size 2.
	opReadRune3        = 3  // Read rune of size 3.
	opReadRune4        = 4  // Read rune of size 4.
)

// ErrTooLarge is passed to panic if memory cannot be allocated to store data in a buffer.
var ErrTooLarge = errors.New("bytes.Buffer: too large")

func NewMappedByteBuffer(mMap mmap.MemoryMap) *MappedByteBuffer {
	mappedByteBuffer := &MappedByteBuffer{}
	mappedByteBuffer.mMapBuf = mMap
	return mappedByteBuffer
}

func (m *MappedByteBuffer) Bytes() []byte { return m.mMapBuf[m.off:] }

// String returns the contents of the unread portion of the buffer
// as a string. If the Buffer is a nil pointer, it returns "<nil>".
func (m *MappedByteBuffer) String() string {
	if m == nil {
		// Special case, useful in debugging.
		return "<nil>"
	}
	return string(m.mMapBuf[m.off:])
}

// Cap returns the capacity of the buffer's underlying byte slice, that is, the
// total space allocated for the buffer's data.
func (m *MappedByteBuffer) Cap() int { return cap(m.mMapBuf) }

// Len returns the number of bytes of the unread portion of the buffer;
// m.Len() == len(m.Bytes()).
func (m *MappedByteBuffer) Len() int { return len(m.mMapBuf) - m.off }

// Truncate discards all but the first n unread bytes from the buffer
// but continues to use the same allocated storage.
// It panics if n is negative or greater than the length of the buffer.
func (m *MappedByteBuffer) Truncate(n int) {
	m.lastRead = opInvalid
	switch {
	case n < 0 || n > m.Len():
		panic("bytes.Buffer: truncation out of range")
	case n == 0:
		// Reuse buffer space.
		m.off = 0
	}
	m.mMapBuf = m.mMapBuf[0: m.off+n]
}

// Reset resets the buffer to be empty,
// but it retains the underlying storage for use by future writes.
// Reset is the same as Truncate(0).
func (m *MappedByteBuffer) Reset() { m.Truncate(0) }

// grow grows the buffer to guarantee space for n more bytes.
// It returns the index where bytes should be written.
// If the buffer can't grow it will panic with ErrTooLarge.
func (b *MappedByteBuffer) grow(n int) int {
	m := b.Len()
	// If buffer is empty, reset to recover space.
	if m == 0 && b.off != 0 {
		b.Truncate(0)
	}
	if len(b.mMapBuf)+n > cap(b.mMapBuf) {
		var buf []byte
		if b.mMapBuf == nil && n <= len(b.bootstrap) {
			buf = b.bootstrap[0:]
		} else if m+n <= cap(b.mMapBuf)/2 {
			// We can slide things down instead of allocating a new
			// slice. We only need m+n <= cap(b.buf) to slide, but
			// we instead let capacity get twice as large so we
			// don't spend all our time copying.
			copy(b.mMapBuf[:], b.mMapBuf[b.off:])
			buf = b.mMapBuf[:m]
		} else {
			// not enough space anywhere
			buf = makeSlice(2*cap(b.mMapBuf) + n)
			copy(buf, b.mMapBuf[b.off:])
		}
		b.mMapBuf = buf
		b.off = 0
	}
	b.mMapBuf = b.mMapBuf[0: b.off+m+n]
	return b.off + m
}

// makeSlice allocates a slice of size n. If the allocation fails, it panics
// with ErrTooLarge.
func makeSlice(n int) []byte {
	// If the make fails, give a known error.
	defer func() {
		if recover() != nil {
			panic(ErrTooLarge)
		}
	}()
	return make([]byte, n)
}

// Grow grows the buffer's capacity, if necessary, to guarantee space for
// another n bytes. After Grow(n), at least n bytes can be written to the
// buffer without another allocation.
// If n is negative, Grow will panic.
// If the buffer can't grow it will panic with ErrTooLarge.
func (b *MappedByteBuffer) Grow(n int) {
	if n < 0 {
		panic("bytes.Buffer.Grow: negative count")
	}
	m := b.grow(n)
	b.mMapBuf = b.mMapBuf[0:m]
}

// Write appends the contents of p to the buffer, growing the buffer as
// needed. The return value n is the length of p; err is always nil. If the
// buffer becomes too large, Write will panic with ErrTooLarge.
func (m *MappedByteBuffer) Write(p []byte) (n int, err error) {
	logger.Info("1-mMapBuf == %p", m.mMapBuf)
	//m.mMapBuf = append(m.mMapBuf, p...)

	logger.Info("2-mMapBuf == %p", m.mMapBuf)
	return len(p), nil
	/*logger.Info("1-mMapBuf == %p", m.mMapBuf)
	m.lastRead = opInvalid
	num := m.grow(len(p))
	logger.Info("2-mMapBuf == %p", m.mMapBuf)
	return copy(m.mMapBuf[num:], p), nil*/
}

// WriteString appends the contents of s to the buffer, growing the buffer as
// needed. The return value n is the length of s; err is always nil. If the
// buffer becomes too large, WriteString will panic with ErrTooLarge.
func (m *MappedByteBuffer) WriteString(s string) (n int, err error) {
	m.lastRead = opInvalid
	num := m.grow(len(s))
	return copy([]byte(m.mMapBuf[num:]), s), nil
}

// Read reads the next len(p) bytes from the buffer or until the buffer
// is drained. The return value n is the number of bytes read. If the
// buffer has no data to return, err is io.EOF (unless len(p) is zero);
// otherwise it is nil.
func (b *MappedByteBuffer) Read(p []byte) (n int, err error) {
	b.lastRead = opInvalid
	if b.off >= len(b.mMapBuf) {
		// Buffer is empty, reset to recover space.
		b.Truncate(0)
		if len(p) == 0 {
			return
		}
		return 0, io.EOF
	}
	n = copy(p, b.mMapBuf[b.off:])
	b.off += n
	if n > 0 {
		b.lastRead = opRead
	}
	return
}

func (self *MappedByteBuffer) Flush() {
	self.mMapBuf.Flush()
}

func (self *MappedByteBuffer) Unmap() {
	self.mMapBuf.Unmap()
}

func (self *MappedByteBuffer) putInt32(i int32) (mappedByteBuffer *MappedByteBuffer) {

	return self
}

func (self *MappedByteBuffer) putInt64(i int64) (mappedByteBuffer *MappedByteBuffer) {
	return self
}

// slice 返回当前MappedByteBuffer.byteBuffer中从开始位置到len的分片buffer
// Author: tantexian, <my.oschina.net/tantexian>
// Since: 17/8/6
func (self *MappedByteBuffer) slice() (*bytes.Buffer) {
	return bytes.NewBuffer(self.mMapBuf[:self.off])
}
